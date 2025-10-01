//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/cast_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/logical_type.hpp"

namespace duckdb_stable {

class CastFunction {
public:
	virtual LogicalType SourceType() = 0;
	virtual LogicalType TargetType() = 0;
	virtual int64_t ImplicitCastCost() = 0;
	virtual duckdb_cast_function_t GetFunction() = 0;
};

} // namespace duckdb_stable
