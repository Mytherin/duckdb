//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/generic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

namespace duckdb_stable {

class string_t {
public:
	const char *GetData() const {

	}
	uint32_t GetSize() const {
		return string.value.inlined.length;
	}
private:
	duckdb_string_t string;
};
}
