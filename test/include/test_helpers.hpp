//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// test_helpers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef _MSC_VER
// these break enum.hpp otherwise
#undef DELETE
#undef DEFAULT
#undef EXISTS
#undef IN
// this breaks file_system.cpp otherwise
#undef CreateDirectory
#undef RemoveDirectory
#endif

#include "compare_result.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types.hpp"
#include "test_config.hpp"
#include <sstream>
#include <iostream>

namespace duckdb {

void RegisterSqllogictests();
bool SummarizeFailures();

void DeleteDatabase(string path);
void TestDeleteDirectory(string path);
void TestCreateDirectory(string path);
string TestJoinPath(string path1, string path2);
void TestDeleteFile(string path);
void TestChangeDirectory(string path);

void SetDeleteTestPath(bool delete_path);
bool DeleteTestPath();
void ClearTestDirectory();
string TestGetCurrentDirectory();
string TestDirectoryPath();
string TestCreatePath(string suffix);
unique_ptr<DBConfig> GetTestConfig();
bool TestIsInternalError(unordered_set<string> &internal_error_messages, const string &error);
void SetTestDirectory(string path);
void SetDebugInitialize(int value);
void AddRequire(string require);
bool IsRequired(string require);

bool NO_FAIL(QueryResult &result);
bool NO_FAIL(duckdb::unique_ptr<QueryResult> result);

#define REQUIRE_NO_FAIL(result) REQUIRE(NO_FAIL((result)))
#define REQUIRE_FAIL(result)    REQUIRE((result)->HasError())

#define COMPARE_CSV(result, csv, header)                                                                               \
	{                                                                                                                  \
		auto res = compare_csv(*result, csv, header);                                                                  \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

#define COMPARE_CSV_COLLECTION(collection, csv, header)                                                                \
	{                                                                                                                  \
		auto res = compare_csv_collection(collection, csv, header);                                                    \
		if (!res.empty())                                                                                              \
			FAIL(res);                                                                                                 \
	}

} // namespace duckdb
