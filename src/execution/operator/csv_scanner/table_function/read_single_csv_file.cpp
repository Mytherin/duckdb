#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/execution/operator/csv_scanner/csv_multi_file_info.hpp"
#include "duckdb/execution/operator/csv_scanner/global_csv_state.hpp"
#include "duckdb/execution/operator/persistent/csv_rejects_table.hpp"
#include "duckdb/function/table/read_csv.hpp"

namespace duckdb {

//! Bind data of read_single_csv_file - the regular CSV read data plus the single file that is read
struct ReadSingleCSVFileData : public ReadCSVData {
	OpenFileInfo file;
	//! The names/types this file is read with
	vector<Identifier> csv_names;
	vector<LogicalType> csv_types;
	//! Whether the schema was determined on other files - the dialect of this file is then still sniffed, and the
	//! sniffer reconciles the result with "csv_schema"
	bool schema_from_other_file = false;
	//! Whether the columns are fixed but the dialect of this file is not known yet - only the dialect is sniffed
	bool sniff_dialect_only = false;
};

struct ReadSingleCSVFileGlobalState : public GlobalTableFunctionState {
public:
	ReadSingleCSVFileGlobalState(ClientContext &context, ReadSingleCSVFileData &csv_data)
	    : state(context, csv_data, csv_data.csv_names, 1) {
	}

public:
	idx_t MaxThreads() const override {
		return max_threads;
	}

public:
	//! The file that is read - declared before the state below, which holds buffers of this file and must therefore
	//! be destroyed first
	shared_ptr<CSVFileScan> file_scan;
	CSVGlobalState state;
	//! Handing out the next part of the file is done single-threadedly
	mutex lock;
	//! Whether we are done handing out parts of the file
	bool finished_launching = false;
	idx_t max_threads = 1;
};

struct ReadSingleCSVFileLocalState : public LocalTableFunctionState {
	CSVLocalState state;
	//! Whether our caller claims the parts of the file we read - see table_function_claim_scan_unit_t
	bool claimed_externally = false;
};

static unique_ptr<FunctionData> ReadSingleCSVFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<ReadSingleCSVFileData>();
	auto &options = result->options;
	for (auto &kv : input.named_parameters) {
		options.ParseOption(context, kv.first, kv.second);
	}
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_single_csv_file requires a non-NULL file name");
	}
	result->file = OpenFileInfo(StringValue::Get(input.inputs[0]));
	options.file_path = result->file.path;

	// the multi-file options only steer the sniffer through union_by_name, which does not apply to a single file
	MultiFileOptions file_options;
	SimpleMultiFileList file_list(vector<OpenFileInfo> {result->file});
	if (input.expected_bind_data && input.HasExpectedSchema()) {
		// the schema of the scan was determined already - read this file using that schema. The dialect of this
		// file is still sniffed when scanning, and reconciled with the schema of the scan
		auto &source = input.expected_bind_data->Cast<ReadSingleCSVFileData>();
		// read this file with the options the schema was determined with - the dialect that was sniffed there is
		// the starting point for this file, whose own dialect is then sniffed and reconciled with "csv_schema"
		result->options = source.options;
		result->options.file_path = result->file.path;
		result->csv_schema = source.csv_schema;
		result->schema_from_other_file = true;
		names = *input.expected_names;
		return_types = *input.expected_types;
	} else if (input.HasExpectedSchema()) {
		// the columns are known but not the dialect of this file - only the dialect is sniffed. This is the case
		// for COPY, which takes its columns from the target table, and for union_by_name
		options.name_list = *input.expected_names;
		options.sql_type_list = *input.expected_types;
		options.columns_set = true;
		options.sql_types_per_column.clear();
		for (idx_t i = 0; i < options.name_list.size(); i++) {
			options.sql_types_per_column[options.name_list[i]] = i;
		}
		names = options.name_list;
		return_types = options.sql_type_list;
		result->sniff_dialect_only = true;
	} else if (options.auto_detect) {
		result->csv_schema = CSVSchemaDiscovery::SchemaDiscovery(context, result->buffer_manager, options, file_options,
		                                                         return_types, names, file_list);
	} else {
		if (!options.columns_set) {
			throw BinderException("read_csv requires columns to be specified through the 'columns' option. Use "
			                      "read_csv_auto or set read_csv(..., AUTO_DETECT=TRUE) to automatically guess "
			                      "columns.");
		}
		names = options.name_list;
		return_types = options.sql_type_list;
	}
	if (return_types.size() != names.size()) {
		throw BinderException("read_csv: mismatch between the number of column names (%d) and column types (%d)",
		                      names.size(), return_types.size());
	}
	options.dialect_options.num_cols = names.size();
	options.Verify(file_options);

	// the equivalent of MultiFileReaderInterface::FinalizeBindData
	if (!options.force_not_null_names.empty()) {
		identifier_set_t column_names;
		for (auto &name : names) {
			column_names.insert(name);
		}
		for (auto &force_name : options.force_not_null_names) {
			if (column_names.find(Identifier(force_name)) == column_names.end()) {
				throw BinderException("\"force_not_null\" expected to find %s, but it was not found in the table",
				                      force_name);
			}
		}
		D_ASSERT(options.force_not_null.empty());
		for (auto &name : names) {
			options.force_not_null.push_back(options.force_not_null_names.find(name.GetIdentifierName()) !=
			                                 options.force_not_null_names.end());
		}
	}
	for (auto &type : return_types) {
		if (type.id() == LogicalTypeId::SQLNULL) {
			// if we cannot tell the type of a column we default to the highest type, a VARCHAR
			type = LogicalType::VARCHAR;
		}
	}
	result->Finalize();
	result->csv_names = names;
	result->csv_types = return_types;
	return std::move(result);
}

//! Combine the schemas of several CSV files the way the multi-file sniffer does - the resulting CSV schema is
//! handed to the bind of every file, whose dialect is then sniffed and reconciled with it
static unique_ptr<FunctionData> ReadSingleCSVFileCombineSchema(ClientContext &context,
                                                               TableFunctionCombineSchemaInput &input,
                                                               vector<LogicalType> &return_types,
                                                               vector<Identifier> &names) {
	if (input.union_by_name) {
		// the files are expected to have different columns - those are unified by name, not reconciled positionally
		return nullptr;
	}
	optional_ptr<const ReadSingleCSVFileData> first_file;
	CSVSchema best_schema;
	for (auto &bind_data : input.bind_data) {
		auto &csv_data = bind_data.get().Cast<ReadSingleCSVFileData>();
		if (csv_data.csv_schema.Empty()) {
			// the schema of this file was not sniffed - fall back to combining the types
			return nullptr;
		}
		if (!first_file) {
			first_file = csv_data;
		}
		auto schema = csv_data.csv_schema;
		if (best_schema.Empty() || best_schema.GetRowsRead() == 0) {
			// a schema is better than no schema, and any schema beats one without data rows
			best_schema = schema;
		} else if (schema.GetRowsRead() != 0) {
			best_schema.MergeSchemas(schema, first_file->options.null_padding);
		}
	}
	if (!first_file) {
		return nullptr;
	}
	best_schema.ReplaceNullWithVarchar();
	names = StringsToIdentifiers(best_schema.GetNames());
	return_types = best_schema.GetTypes();

	auto result = make_uniq<ReadSingleCSVFileData>();
	// the options that were sniffed on the first file are the starting point for every file of the scan. The
	// columns are not set on them - they are carried by the CSV schema, which each file is reconciled with
	result->options = first_file->options;
	result->options.dialect_options.num_cols = names.size();
	// the buffer manager of the first file is kept, like the multi-file sniffer does - it tells the scan whether
	// the files can be read ahead, and the first file does not need to be opened again
	result->buffer_manager = first_file->buffer_manager;
	result->csv_schema = best_schema;
	result->csv_names = names;
	result->csv_types = return_types;
	result->Finalize();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ReadSingleCSVFileInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto &csv_data = input.bind_data->CastNoConst<ReadSingleCSVFileData>();

	// create the temporary rejects table
	if (csv_data.options.store_rejects.GetValue()) {
		CSVRejectsTable::GetOrCreate(context, csv_data.options.rejects_scan_name.GetValue(),
		                             csv_data.options.rejects_table_name.GetValue())
		    ->InitializeTable(context, csv_data);
	}

	auto result = make_uniq<ReadSingleCSVFileGlobalState>(context, csv_data);

	auto options = csv_data.options;
	if (csv_data.sniff_dialect_only) {
		// the columns are fixed - the scanner only sniffs the dialect of this file
		options.auto_detect = true;
	} else if (!csv_data.schema_from_other_file) {
		// this file determined the schema of the scan, so it has been sniffed already
		options.auto_detect = false;
	}
	MultiFileOptions file_options;
	result->file_scan = make_shared_ptr<CSVFileScan>(
	    context, csv_data.file, std::move(options), file_options, csv_data.csv_names, csv_data.csv_types,
	    csv_data.csv_schema, result->state.SingleThreadedRead(), csv_data.buffer_manager, csv_data.sniff_dialect_only);

	// perform projection pushdown - the scanner emits the columns in the order they are requested
	auto &file_scan = *result->file_scan;
	for (auto &column_index : input.column_indexes) {
		const auto col_id = column_index.GetPrimaryIndex();
		if (IsVirtualColumn(col_id)) {
			continue;
		}
		file_scan.column_ids.push_back(MultiFileLocalColumnId(col_id));
	}
	file_scan.file_list_idx = 0;
	// the dialect sniffer may have settled on different types for this file - the scan produces the types the bind
	// promised, and the scanner converts to them while parsing
	auto &file_types = file_scan.GetTypes();
	for (idx_t col_id = 0; col_id < file_types.size() && col_id < csv_data.csv_types.size(); col_id++) {
		if (file_types[col_id] != csv_data.csv_types[col_id]) {
			file_scan.cast_map[col_id] = csv_data.csv_types[col_id];
		}
	}
	file_scan.InitializeFileNamesTypes();
	file_scan.SetStart();

	if (!result->state.SingleThreadedRead()) {
		const idx_t bytes_per_thread = CSVIterator::BytesPerThread(csv_data.options);
		result->max_threads = file_scan.file_size / bytes_per_thread + 1;
	}
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ReadSingleCSVFileInitLocal(ExecutionContext &context,
                                                                      TableFunctionInitInput &input,
                                                                      GlobalTableFunctionState *global_state) {
	return make_uniq<ReadSingleCSVFileLocalState>();
}

//! Assign the next part of the file to this thread
static bool ClaimNextPart(ReadSingleCSVFileGlobalState &gstate, ReadSingleCSVFileLocalState &lstate) {
	lock_guard<mutex> guard(gstate.lock);
	gstate.state.FinishScan(std::move(lstate.state.csv_reader));
	lstate.state.claim_state = CSVLocalState::ClaimState::IDLE;
	if (gstate.finished_launching) {
		return false;
	}
	if (gstate.state.Next(gstate.file_scan, lstate.state)) {
		return true;
	}
	// we have handed out the entire file - this is also where the errors of the file are reported
	gstate.finished_launching = true;
	gstate.state.FinishLaunchingTasks(*gstate.file_scan);
	return false;
}

static bool ReadSingleCSVFileClaimScanUnit(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleCSVFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleCSVFileLocalState>();
	// our caller hands out the parts of the file, so we must not claim the next one ourselves
	lstate.claimed_externally = true;
	return ClaimNextPart(gstate, lstate);
}

//! The CSV scanner can be read ahead when the buffers of the file can be addressed individually
static bool ReadSingleCSVFileSupportsReadAhead(const FunctionData &bind_data) {
	auto &csv_data = bind_data.Cast<ReadSingleCSVFileData>();
	return csv_data.buffer_manager && csv_data.buffer_manager->file_handle &&
	       csv_data.buffer_manager->file_handle->HasKnownBufferRanges();
}

//! Load the buffers of the claimed part of the file that are not in memory yet
static AsyncResult ReadSingleCSVFileScheduleIO(ClientContext &context, TableFunctionInput &input) {
	auto &lstate = input.local_state->Cast<ReadSingleCSVFileLocalState>();
	if (lstate.state.claim_state != CSVLocalState::ClaimState::PENDING) {
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return AsyncResult::FromTasks(CSVCollectClaimIOTasks(lstate.state), TaskSchedulerType::ASYNC);
}

//! Release the part of the file this thread was reading
static void ReadSingleCSVFileFinishScan(ClientContext &context, TableFunctionInput &input) {
	auto &gstate = input.global_state->Cast<ReadSingleCSVFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleCSVFileLocalState>();
	lock_guard<mutex> guard(gstate.lock);
	gstate.state.FinishScan(std::move(lstate.state.csv_reader));
}

static void ReadSingleCSVFileFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &gstate = input.global_state->Cast<ReadSingleCSVFileGlobalState>();
	auto &lstate = input.local_state->Cast<ReadSingleCSVFileLocalState>();

	while (true) {
		if (lstate.state.claim_state == CSVLocalState::ClaimState::IDLE) {
			if (lstate.claimed_externally) {
				// the next part of the file is claimed by our caller
				return;
			}
			if (!ClaimNextPart(gstate, lstate)) {
				// there is nothing left for us to read in this file
				return;
			}
		}
		if (lstate.state.claim_state == CSVLocalState::ClaimState::PENDING) {
			lstate.state.Materialize();
		}
		auto &csv_reader = *lstate.state.csv_reader;
		if (csv_reader.IsSuspended() || !csv_reader.FinishedIterator()) {
			csv_reader.Flush(output);
			if (csv_reader.IsSuspended()) {
				// the scanner needs a buffer that is not in memory - load it and resume
				csv_reader.buffer_manager->GetBuffer(csv_reader.PendingBufferIdx());
				continue;
			}
			if (output.size() != 0) {
				return;
			}
		}
		// this part of the file is done - grab the next one
		lock_guard<mutex> guard(gstate.lock);
		gstate.state.FinishScan(std::move(lstate.state.csv_reader));
		lstate.state.claim_state = CSVLocalState::ClaimState::IDLE;
	}
}

static double ReadSingleCSVFileProgress(ClientContext &context, const FunctionData *bind_data,
                                        const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	auto &gstate = global_state->Cast<ReadSingleCSVFileGlobalState>();
	if (!gstate.file_scan) {
		return 0;
	}
	return gstate.file_scan->GetProgressInFile(context);
}

static unique_ptr<NodeStatistics> ReadSingleCSVFileCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &csv_data = bind_data->Cast<ReadSingleCSVFileData>();
	// determined through the scientific method as the average amount of rows in a CSV file
	idx_t per_file_cardinality = 42;
	if (csv_data.buffer_manager && csv_data.buffer_manager->file_handle) {
		auto estimated_row_width = csv_data.csv_types.size() * 5;
		per_file_cardinality = csv_data.buffer_manager->file_handle->FileSize() / estimated_row_width;
	}
	return make_uniq<NodeStatistics>(per_file_cardinality);
}

TableFunction ReadCSVTableFunction::GetSingleFileFunction() {
	TableFunction read_csv("read_single_csv_file", {LogicalType::VARCHAR}, ReadSingleCSVFileFunction,
	                       ReadSingleCSVFileBind, ReadSingleCSVFileInitGlobal, ReadSingleCSVFileInitLocal);
	read_csv.combine_schema = ReadSingleCSVFileCombineSchema;
	read_csv.claim_scan_unit = ReadSingleCSVFileClaimScanUnit;
	read_csv.finish_scan = ReadSingleCSVFileFinishScan;
	read_csv.supports_read_ahead = ReadSingleCSVFileSupportsReadAhead;
	read_csv.schedule_io = ReadSingleCSVFileScheduleIO;
	read_csv.table_scan_progress = ReadSingleCSVFileProgress;
	read_csv.cardinality = ReadSingleCSVFileCardinality;
	read_csv.projection_pushdown = true;
	ReadCSVAddNamedParameters(read_csv);
	return read_csv;
}

TableFunction ReadCSVTableFunction::GetMultiFileFunction(Identifier name) {
	// the multi-file CSV reader is the single-file CSV reader wrapped into a multi-file function
	TableFunctionMultiFileSettings settings;
	settings.glob_input = FileGlobInput(FileGlobOptions::FALLBACK_GLOB, "csv");
	settings.reader_type = "CSV";
	// like read_csv, the schema is determined by combining the schemas of up to "files_to_sniff" files
	settings.maximum_sample_files = 10;
	settings.sample_files_parameter = "files_to_sniff";
	return TableFunctionMultiFileWrapper::CreateFunction(GetSingleFileFunction(), std::move(name), std::move(settings));
}

} // namespace duckdb
