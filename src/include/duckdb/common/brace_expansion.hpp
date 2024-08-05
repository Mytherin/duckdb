//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/brace_expansion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class BraceExpansion {
public:
	static void ExpandBraces(const string &pattern, vector<string> &result);
};

} // namespace duckdb
