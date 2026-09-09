//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/csv_rejects_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/object_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

struct ReadCSVData;
class TableCatalogEntry;
class ClientContext;

class CSVRejectsTable : public ObjectCacheEntry {
public:
	CSVRejectsTable(string rejects_scan, string rejects_error)
	    : count(0), scan_table(std::move(rejects_scan)), errors_table(std::move(rejects_error)) {
	}
	mutex write_lock;
	string name;
	idx_t count;
	string scan_table;
	string errors_table;

	static shared_ptr<CSVRejectsTable> GetOrCreate(ClientContext &context, const string &rejects_scan,
	                                               const string &rejects_error);

	void InitializeTable(ClientContext &context, const ReadCSVData &options);
	TableCatalogEntry &GetErrorsTable(ClientContext &context);
	TableCatalogEntry &GetScansTable(ClientContext &context);

	static string ObjectType() {
		return "csv_rejects_table_cache";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	// Rejects table records the overall error counts, which is relatively small and should not be evicted.
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx {};
	}

private:
	//! Current File Index being used in the query
	//! Current Query ID being executed
};

} // namespace duckdb
