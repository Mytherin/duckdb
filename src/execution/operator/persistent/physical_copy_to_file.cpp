#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include <algorithm>

namespace duckdb {

struct PartitionWriteInfo {
	unique_ptr<GlobalFunctionData> global_state;
};

struct VectorOfValuesHashFunction {
	uint64_t operator()(const vector<Value> &values) const {
		hash_t result = 0;
		for (auto &val : values) {
			result ^= val.Hash();
		}
		return result;
	}
};

struct VectorOfValuesEquality {
	bool operator()(const vector<Value> &a, const vector<Value> &b) const {
		if (a.size() != b.size()) {
			return false;
		}
		for (idx_t i = 0; i < a.size(); i++) {
			if (ValueOperations::DistinctFrom(a[i], b[i])) {
				return false;
			}
		}
		return true;
	}
};

template <class T>
using vector_of_value_map_t = unordered_map<vector<Value>, T, VectorOfValuesHashFunction, VectorOfValuesEquality>;

class CopyToFunctionGlobalState : public GlobalSinkState {
public:
	explicit CopyToFunctionGlobalState(unique_ptr<GlobalFunctionData> global_state)
	    : rows_copied(0), last_file_offset(0), global_state(std::move(global_state)) {
	}
	StorageLock lock;
	atomic<idx_t> rows_copied;
	atomic<idx_t> last_file_offset;
	unique_ptr<GlobalFunctionData> global_state;
	idx_t created_directories = 0;

	//! shared state for HivePartitionedColumnData
	shared_ptr<GlobalHivePartitionState> partition_state;
	unique_ptr<HivePartitionedColumnData> partition_collection;
	vector<unique_ptr<HivePartitionedColumnData>> append_buffers;
	vector<unique_ptr<PartitionedColumnDataAppendState>> append_states;

	idx_t append_count = 0;
	//! The number of in-progress partitions (used for task division)
	atomic<idx_t> in_progress_partitions { 0 };
	//! Number of finished partitions
	atomic<idx_t> finished_partitions { 0 };
	//! Total number of partitions that need to be flushed
	idx_t partition_count = 0;
	mutex sink_lock;
	atomic<idx_t> appending_threads { 0 };
	atomic<idx_t> threads_waiting_to_flush { 0 };
	atomic<idx_t> flush_iteration { 0 };

	void InitializePartitionState(ClientContext &context, const PhysicalCopyToFile &op) {
		partition_collection = make_uniq<HivePartitionedColumnData>(context, op.expected_types, op.partition_columns,
																		   partition_state);
		append_count = 0;
	}

	void ResertPartitionState(ClientContext &context, const PhysicalCopyToFile &op) {
		append_states.clear();
		append_buffers.clear();
		partition_collection.reset();
		InitializePartitionState(context, op);
	}

	void PrepareForFlush(ClientContext &context, const PhysicalCopyToFile &op) {
		// merge all partitions in into the main collection
		for(idx_t buffer_idx = 0; buffer_idx < append_buffers.size(); buffer_idx++) {
			auto &buffer = *append_buffers[buffer_idx];
			auto &append_state = *append_states[buffer_idx];
			// first flush the append
			buffer.FlushAppendState(append_state);
			// then combine into the main collection
			partition_collection->Combine(buffer);
		}
		partition_collection->SynchronizeLocalMap();
		in_progress_partitions = 0;
		finished_partitions = 0;
		partition_count = partition_collection->GetPartitions().size();

		// ensure all partition directories are created before we start writing
		CreatePartitionDirectories(context, op);
	}

	void FlushPartitions(ExecutionContext &context, const PhysicalCopyToFile &op, CopyToFunctionGlobalState &g) {
		auto &partitions = partition_collection->GetPartitions();
		auto partition_key_map = partition_collection->GetReverseMap();
		while(true) {
			// obtain a new partition
			auto partition_idx = in_progress_partitions++;
			if (partition_idx >= partition_count) {
				// we exhausted all partitions - finished
				break;
			}
			// flush the partition
			// get the partition write info for this buffer
			D_ASSERT(partition_key_map.find(partition_idx) != partition_key_map.end());
			auto &info = g.GetPartitionWriteInfo(context, op, partition_key_map[partition_idx]->values);

			auto local_copy_state = op.function.copy_to_initialize_local(context, *op.bind_data);
			// push the chunks into the write state
			for (auto &chunk : partitions[partition_idx]->Chunks()) {
				op.function.copy_to_sink(context, *op.bind_data, *info.global_state, *local_copy_state, chunk);
			}
			op.function.copy_to_combine(context, *op.bind_data, *info.global_state, *local_copy_state);
			local_copy_state.reset();
			partitions[partition_idx].reset();
			auto finished_idx = ++finished_partitions;
			D_ASSERT(finished_idx <= partition_count);
			if (finished_idx == partition_count) {
				// we are the last partition to finish
				// reset the partition state
				g.ResertPartitionState(context.client, op);
				// increment the finished_partitions, this allows all threads to continue appending
				++finished_partitions;
				break;
			}
		}
		// we have finished, but maybe there are still other partitions remaining
		// busy loop until all flushing is completed
		while(finished_partitions <= partition_count) {
		}
	}

	static void CreateDir(const string &dir_path, FileSystem &fs) {
		if (!fs.DirectoryExists(dir_path)) {
			fs.CreateDirectory(dir_path);
		}
	}

	static void CreateDirectories(const vector<idx_t> &cols, const vector<string> &names, const vector<Value> &values,
	                              string path, FileSystem &fs) {
		CreateDir(path, fs);

		for (idx_t i = 0; i < cols.size(); i++) {
			const auto &partition_col_name = names[cols[i]];
			const auto &partition_value = values[i];
			string p_dir = partition_col_name + "=" + partition_value.ToString();
			path = fs.JoinPath(path, p_dir);
			CreateDir(path, fs);
		}
	}

	void CreatePartitionDirectories(ClientContext &context, const PhysicalCopyToFile &op) {
		auto &fs = FileSystem::GetFileSystem(context);

		auto trimmed_path = op.GetTrimmedPath(context);

		lock_guard<mutex> global_lock_on_partition_state(partition_state->lock);
		const auto &global_partitions = partition_state->partitions;
		// global_partitions have partitions added only at the back, so it's fine to only traverse the last part

		for (idx_t i = created_directories; i < global_partitions.size(); i++) {
			CreateDirectories(op.partition_columns, op.names, global_partitions[i]->first.values, trimmed_path, fs);
		}
		created_directories = global_partitions.size();
	}

	static string GetDirectory(const vector<idx_t> &cols, const vector<string> &names, const vector<Value> &values,
	                           string path, FileSystem &fs) {
		for (idx_t i = 0; i < cols.size(); i++) {
			const auto &partition_col_name = names[cols[i]];
			const auto &partition_value = values[i];
			string p_dir = partition_col_name + "=" + partition_value.ToString();
			path = fs.JoinPath(path, p_dir);
		}
		return path;
	}

	void FinalizePartition(ClientContext &context, const PhysicalCopyToFile &op, PartitionWriteInfo &info) {
		if (!info.global_state) {
			// already finalized
			return;
		}
		// finalize the partition
		op.function.copy_to_finalize(context, *op.bind_data, *info.global_state);
		info.global_state.reset();
	}

	void FinalizePartitions(ClientContext &context, const PhysicalCopyToFile &op, Pipeline &pipeline) {
		// flush any remaining partitions
		if (partition_state) {
			// FIXME: we could launch threads here
			PrepareForFlush(context, op);
			ThreadContext thread(context);
			ExecutionContext execution_context(context, thread, &pipeline);
			FlushPartitions(execution_context, op, *this);
		}
		// finalize any remaining partitions
		for (auto &entry : active_partitioned_writes) {
			FinalizePartition(context, op, *entry.second);
		}
	}

	PartitionWriteInfo &GetPartitionWriteInfo(ExecutionContext &context, const PhysicalCopyToFile &op,
	                                          const vector<Value> &values) {
		// check if we have already started writing this partition
		{
			lock_guard<mutex> l(active_partitioned_writes_lock);
			auto entry = active_partitioned_writes.find(values);
			if (entry != active_partitioned_writes.end()) {
				// we have - continue writing in this partition
				return *entry->second;
			}
		}
		auto &fs = FileSystem::GetFileSystem(context.client);
		// Create a writer for the current file
		auto trimmed_path = op.GetTrimmedPath(context.client);
		string hive_path = GetDirectory(op.partition_columns, op.names, values, trimmed_path, fs);
		string full_path(op.filename_pattern.CreateFilename(fs, hive_path, op.file_extension, 0));
		if (fs.FileExists(full_path) && !op.overwrite_or_ignore) {
			throw IOException("failed to create %s, file exists! Enable OVERWRITE_OR_IGNORE option to force writing",
			                  full_path);
		}
		// initialize writes
		auto info = make_uniq<PartitionWriteInfo>();
		info->global_state = op.function.copy_to_initialize_global(context.client, *op.bind_data, full_path);
		auto &result = *info;
		// store in active write map
		lock_guard<mutex> l(active_partitioned_writes_lock);
		active_partitioned_writes.insert(make_pair(values, std::move(info)));
		return result;
	}

private:
	mutex active_partitioned_writes_lock;
	//! The active writes per partition (for partitioned write)
	vector_of_value_map_t<unique_ptr<PartitionWriteInfo>> active_partitioned_writes;
};

string PhysicalCopyToFile::GetTrimmedPath(ClientContext &context) const {
	auto &fs = FileSystem::GetFileSystem(context);
	string trimmed_path = file_path;
	StringUtil::RTrim(trimmed_path, fs.PathSeparator(trimmed_path));
	return trimmed_path;
}

class CopyToFunctionLocalState : public LocalSinkState {
public:
	explicit CopyToFunctionLocalState(unique_ptr<LocalFunctionData> local_state)
	    : local_state(std::move(local_state)) {
	}
	unique_ptr<GlobalFunctionData> global_state;
	unique_ptr<LocalFunctionData> local_state;

	//! Buffers the tuples in partitions before writing
	optional_ptr<HivePartitionedColumnData> part_buffer;
	optional_ptr<PartitionedColumnDataAppendState> part_buffer_append_state;
	idx_t current_flush_iteration = 0;

	void InitializeAppendState(CopyToFunctionGlobalState &gstate) {
		auto lock = gstate.lock.GetExclusiveLock();
		auto partition_buffer = unique_ptr_cast<PartitionedColumnData, HivePartitionedColumnData>(gstate.partition_collection->CreateShared());;
		auto append_state = make_uniq<PartitionedColumnDataAppendState>();
		part_buffer = partition_buffer.get();
		part_buffer_append_state = append_state.get();
		part_buffer->InitializeAppendState(*part_buffer_append_state);
		gstate.append_buffers.push_back(std::move(partition_buffer));
		gstate.append_states.push_back(std::move(append_state));
		current_flush_iteration = gstate.flush_iteration;
	}

	void FlushPartitions(ExecutionContext &context, const PhysicalCopyToFile &op, CopyToFunctionGlobalState &g) {
		// start flushing - this will busy spin until ALL flushing is completed
		g.FlushPartitions(context, op, g);
		// reset the local append state
		ResetAppendState();
	}

	void AppendToPartition(ExecutionContext &context, const PhysicalCopyToFile &op, CopyToFunctionGlobalState &g,
	                       DataChunk &chunk) {
		g.appending_threads++;
		if (current_flush_iteration != g.flush_iteration) {
			// a flush has started (OR finished) since our last append
			// either help with flushing, or just reset the append state if flushing is already done
			FlushPartitions(context, op, g);
		} else if (g.append_count >= ClientConfig::GetConfig(context.client).partitioned_write_flush_threshold) {
			// we are past the threshold for writing to disk but a flush has not started yet
			// we cannot start a flush while threads are still appending, however
			g.threads_waiting_to_flush++;
			{
				// wait for the sink lock
				lock_guard<mutex> l(g.sink_lock);
				if (current_flush_iteration == g.flush_iteration) {
					// we are the first thread to reach this point
					// wait until all other threads have EITHER blocked on the sink lock OR are finished appending
					while(g.appending_threads > g.threads_waiting_to_flush) {
					}
					// we can finally flush!
					// prepare for flush
					g.PrepareForFlush(context.client, op);
					// move to the next flush iteration and start flushing
					g.flush_iteration++;
				}
				g.threads_waiting_to_flush--;
			}
			// now start flushing
			FlushPartitions(context, op, g);
		}
		if (!part_buffer) {
			// re-initialize the append state
			InitializeAppendState(g);
		}
		part_buffer->Append(*part_buffer_append_state, chunk);
		g.append_count += chunk.size();
		g.appending_threads--;
	}

	void ResetAppendState() {
		part_buffer_append_state = nullptr;
		part_buffer = nullptr;
	}
};

unique_ptr<GlobalFunctionData> PhysicalCopyToFile::CreateFileState(ClientContext &context,
                                                                   GlobalSinkState &sink) const {
	auto &g = sink.Cast<CopyToFunctionGlobalState>();
	idx_t this_file_offset = g.last_file_offset++;
	auto &fs = FileSystem::GetFileSystem(context);
	string output_path(filename_pattern.CreateFilename(fs, file_path, file_extension, this_file_offset));
	if (fs.FileExists(output_path) && !overwrite_or_ignore) {
		throw IOException("%s exists! Enable OVERWRITE_OR_IGNORE option to force writing", output_path);
	}
	return function.copy_to_initialize_global(context, *bind_data, output_path);
}

unique_ptr<LocalSinkState> PhysicalCopyToFile::GetLocalSinkState(ExecutionContext &context) const {
	if (partition_output) {
		auto state = make_uniq<CopyToFunctionLocalState>(nullptr);
		return std::move(state);
	}
	auto res = make_uniq<CopyToFunctionLocalState>(function.copy_to_initialize_local(context, *bind_data));
	if (per_thread_output) {
		res->global_state = CreateFileState(context.client, *sink_state);
	}
	return std::move(res);
}

unique_ptr<GlobalSinkState> PhysicalCopyToFile::GetGlobalSinkState(ClientContext &context) const {

	if (partition_output || per_thread_output || file_size_bytes.IsValid()) {
		auto &fs = FileSystem::GetFileSystem(context);

		if (fs.FileExists(file_path) && !overwrite_or_ignore) {
			throw IOException("%s exists! Enable OVERWRITE_OR_IGNORE option to force writing", file_path);
		}
		if (!fs.DirectoryExists(file_path)) {
			fs.CreateDirectory(file_path);
		} else if (!overwrite_or_ignore) {
			idx_t n_files = 0;
			fs.ListFiles(file_path, [&n_files](const string &path, bool) { n_files++; });
			if (n_files > 0) {
				throw IOException("Directory %s is not empty! Enable OVERWRITE_OR_IGNORE option to force writing",
				                  file_path);
			}
		}

		auto state = make_uniq<CopyToFunctionGlobalState>(nullptr);
		if (!per_thread_output && file_size_bytes.IsValid()) {
			state->global_state = CreateFileState(context, *state);
		}

		if (partition_output) {
			state->partition_state = make_shared_ptr<GlobalHivePartitionState>();
			state->InitializePartitionState(context, *this);
		}

		return std::move(state);
	}

	return make_uniq<CopyToFunctionGlobalState>(function.copy_to_initialize_global(context, *bind_data, file_path));
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
void PhysicalCopyToFile::MoveTmpFile(ClientContext &context, const string &tmp_file_path) {
	auto &fs = FileSystem::GetFileSystem(context);

	auto path = StringUtil::GetFilePath(tmp_file_path);
	auto base = StringUtil::GetFileName(tmp_file_path);

	auto prefix = base.find("tmp_");
	if (prefix == 0) {
		base = base.substr(4);
	}

	auto file_path = fs.JoinPath(path, base);
	if (fs.FileExists(file_path)) {
		fs.RemoveFile(file_path);
	}
	fs.MoveFile(tmp_file_path, file_path);
}

PhysicalCopyToFile::PhysicalCopyToFile(vector<LogicalType> types, CopyFunction function_p,
                                       unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::COPY_TO_FILE, std::move(types), estimated_cardinality),
      function(std::move(function_p)), bind_data(std::move(bind_data)), parallel(false) {
}

SinkResultType PhysicalCopyToFile::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (partition_output) {
		l.AppendToPartition(context, *this, g, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	g.rows_copied += chunk.size();

	if (per_thread_output) {
		auto &gstate = l.global_state;
		function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);

		if (file_size_bytes.IsValid() && function.file_size_bytes(*gstate) > file_size_bytes.GetIndex()) {
			function.copy_to_finalize(context.client, *bind_data, *gstate);
			gstate = CreateFileState(context.client, *sink_state);
		}
		return SinkResultType::NEED_MORE_INPUT;
	}

	if (!file_size_bytes.IsValid()) {
		function.copy_to_sink(context, *bind_data, *g.global_state, *l.local_state, chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	// FILE_SIZE_BYTES is set, but threads write to the same file, synchronize using lock
	auto &gstate = g.global_state;
	auto lock = g.lock.GetExclusiveLock();
	if (function.file_size_bytes(*gstate) > file_size_bytes.GetIndex()) {
		auto owned_gstate = std::move(gstate);
		gstate = CreateFileState(context.client, *sink_state);
		lock.reset();
		function.copy_to_finalize(context.client, *bind_data, *owned_gstate);
	} else {
		lock.reset();
	}

	lock = g.lock.GetSharedLock();
	function.copy_to_sink(context, *bind_data, *gstate, *l.local_state, chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCopyToFile::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &g = input.global_state.Cast<CopyToFunctionGlobalState>();
	auto &l = input.local_state.Cast<CopyToFunctionLocalState>();

	if (!partition_output && function.copy_to_combine) {
		if (per_thread_output) {
			// For PER_THREAD_OUTPUT, we can combine/finalize immediately
			function.copy_to_combine(context, *bind_data, *l.global_state, *l.local_state);
			function.copy_to_finalize(context.client, *bind_data, *l.global_state);
		} else if (file_size_bytes.IsValid()) {
			// File in global state may change with FILE_SIZE_BYTES, need to grab lock
			auto lock = g.lock.GetSharedLock();
			function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
		} else {
			function.copy_to_combine(context, *bind_data, *g.global_state, *l.local_state);
		}
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCopyToFile::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                              OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<CopyToFunctionGlobalState>();
	if (partition_output) {
		// finalize all outstanding partitions
		gstate.FinalizePartitions(context, *this, pipeline);
		return SinkFinalizeType::READY;
	}
	if (per_thread_output) {
		// already happened in combine
		return SinkFinalizeType::READY;
	}
	if (function.copy_to_finalize) {
		function.copy_to_finalize(context, *bind_data, *gstate.global_state);

		if (use_tmp_file) {
			D_ASSERT(!per_thread_output);
			D_ASSERT(!partition_output);
			D_ASSERT(!file_size_bytes.IsValid());
			MoveTmpFile(context, file_path);
		}
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

SourceResultType PhysicalCopyToFile::GetData(ExecutionContext &context, DataChunk &chunk,
                                             OperatorSourceInput &input) const {
	auto &g = sink_state->Cast<CopyToFunctionGlobalState>();

	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(g.rows_copied.load())));

	return SourceResultType::FINISHED;
}

} // namespace duckdb
