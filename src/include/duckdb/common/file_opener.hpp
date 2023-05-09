//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/file_opener.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class ClientContext;
class Value;

//! Abstract type that provide client-specific context to FileSystem.
class FileOpener {
public:
	virtual ~FileOpener() {};

	virtual bool TryGetCurrentSetting(const string &key, Value &result) = 0;
	virtual optional_ptr<ClientContext> TryGetClientContext() = 0;
	virtual void SetOption(string key, Value val);

	DUCKDB_API static optional_ptr<ClientContext> TryGetClientContext(optional_ptr<FileOpener> opener);
	DUCKDB_API static bool TryGetCurrentSetting(optional_ptr<FileOpener> opener, const string &key, Value &result);
};

} // namespace duckdb
