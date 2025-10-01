//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/cast_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/logical_type.hpp"
#include "duckdb/stable/generic_executor.hpp"
#include "duckdb/stable/vector.hpp"

namespace duckdb_stable {

class CastFunction {
public:
	virtual LogicalType SourceType() = 0;
	virtual LogicalType TargetType() = 0;
	virtual int64_t ImplicitCastCost() = 0;
	virtual duckdb_cast_function_t GetFunction() = 0;

	template <class OP>
	static bool GetCastFunction(duckdb_function_info info, idx_t count, duckdb_vector input, duckdb_vector output) {
		using SOURCE_TYPE = typename OP::SOURCE_TYPE;
		using TARGET_TYPE = typename OP::TARGET_TYPE;

		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count, [&](const typename SOURCE_TYPE::ARG_TYPE &input) { return OP::Cast(input); });
		return executor.Success();
	}

	template <class OP>
	static bool GetCastFunctionStatic(duckdb_function_info info, idx_t count, duckdb_vector input,
	                                  duckdb_vector output) {
		using SOURCE_TYPE = typename OP::SOURCE_TYPE;
		using TARGET_TYPE = typename OP::TARGET_TYPE;

		CastExecutor executor(info);
		Vector input_vec(input);
		Vector output_vec(output);

		typename OP::STATIC_DATA static_data;
		executor.ExecuteUnary<SOURCE_TYPE, TARGET_TYPE>(
		    input_vec, output_vec, count,
		    [&](const typename SOURCE_TYPE::ARG_TYPE &input) { return OP::Cast(input, static_data); });
		return executor.Success();
	}
};

} // namespace duckdb_stable
