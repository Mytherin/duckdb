//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/union_by_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

template<class READER_TYPE, class OPTION_TYPE>
struct UnionByReaderTask : public Task {
	UnionByReaderTask(ClientContext &context, const string &file, idx_t file_idx, vector<unique_ptr<READER_TYPE>> &readers, vector<ErrorData> &errors, OPTION_TYPE &options, atomic<idx_t> &completed_tasks) :
		context(context), file_name(file), file_idx(file_idx), readers(readers), errors(errors), options(options), completed_tasks(completed_tasks) {
	}

	ClientContext &context;
	const string &file_name;
	idx_t file_idx;
	vector<unique_ptr<READER_TYPE>> &readers;
	vector<ErrorData> &errors;
	OPTION_TYPE &options;
	atomic<idx_t> &completed_tasks;

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		try {
			readers[file_idx] = make_uniq<READER_TYPE>(context, file_name, options);
		} catch(std::exception &ex) {
			errors[file_idx] = ErrorData(ex);
		} catch(...) {
			errors[file_idx] = ErrorData("Unknown error");
		}
		++completed_tasks;
		return TaskExecutionResult::TASK_FINISHED;
	}
};

class UnionByName {
public:
	static void CombineUnionTypes(const vector<string> &new_names, const vector<LogicalType> &new_types,
	                              vector<LogicalType> &union_col_types, vector<string> &union_col_names,
	                              case_insensitive_map_t<idx_t> &union_names_map);

	//! Union all files(readers) by their col names
	template <class READER_TYPE, class OPTION_TYPE>
	static vector<unique_ptr<READER_TYPE>> UnionCols(ClientContext &context, const vector<string> &files,
	                                                 vector<LogicalType> &union_col_types,
	                                                 vector<string> &union_col_names, OPTION_TYPE &options) {
		vector<unique_ptr<READER_TYPE>> union_readers;
		vector<ErrorData> errors;
		case_insensitive_map_t<idx_t> union_names_map;
		auto &scheduler = TaskScheduler::GetScheduler(context);
		auto producer = scheduler.CreateProducer();

		union_readers.resize(files.size());
		errors.resize(files.size());
		atomic<idx_t> completed_tasks(0);
		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			const auto &file_name = files[file_idx];
			auto task = make_uniq<UnionByReaderTask<READER_TYPE, OPTION_TYPE>>(context, file_name, file_idx,
																			   union_readers, errors, options,
																			   completed_tasks);
			scheduler.ScheduleTask(*producer, std::move(task));
		}

		// complete tasks
		shared_ptr<Task> task;
		while(scheduler.GetTaskFromProducer(*producer, task)) {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
		}

		// busy loop until all tasks are completed
		while(completed_tasks < files.size()) {
		}

		for (idx_t file_idx = 0; file_idx < files.size(); ++file_idx) {
			auto &reader = union_readers[file_idx];
			if (!reader) {
				errors[file_idx].Throw();
			}
			auto &col_names = reader->GetNames();
			auto &sql_types = reader->GetTypes();
			CombineUnionTypes(col_names, sql_types, union_col_types, union_col_names, union_names_map);
		}
		return union_readers;
	}
};

} // namespace duckdb
