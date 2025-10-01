//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/string_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"

namespace duckdb_stable {

class string_t {
public:
	const char *GetData() const {
		return IsInlined() ? string.value.inlined.inlined : string.value.pointer.ptr;
	}
	uint32_t GetSize() const {
		return string.value.inlined.length;
	}
	bool IsInlined() const {
		return GetSize() <= 12;
	}
private:
	duckdb_string_t string;
};
}
