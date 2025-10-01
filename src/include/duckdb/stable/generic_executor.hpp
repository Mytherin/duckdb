//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/generic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/data_chunk.hpp"
#include "duckdb/stable/vector.hpp"
#include <functional>

namespace duckdb_stable {
class ExpressionState;

template<class T>
struct ResultValue {
	T val;
	bool is_null;
	bool error;
};


struct PrimitiveTypeState {
	void *data;
	uint64_t *validity;

	void PrepareVector(Vector &input, idx_t count) {
	}
};

template <class INPUT_TYPE>
struct PrimitiveType {
	PrimitiveType() = default;
	PrimitiveType(INPUT_TYPE val) : val(val) { // NOLINT: allow implicit cast
	}

	INPUT_TYPE val;

	using ARG_TYPE = INPUT_TYPE;
	using STRUCT_STATE = PrimitiveTypeState;

	static bool ConstructType(STRUCT_STATE &state, idx_t i, PrimitiveType<INPUT_TYPE> &result) {
		auto &vdata = state.main_data;
		auto idx = vdata.sel->get_index(i);
		auto ptr = UnifiedVectorFormat::GetData<INPUT_TYPE>(vdata);
		result.val = ptr[idx];
		return true;
	}

	static void AssignResult(Vector &result, idx_t i, PrimitiveType<INPUT_TYPE> value) {
		auto result_data = FlatVector::GetData<INPUT_TYPE>(result);
		result_data[i] = value.val;
	}
};

class GenericExecutor {
public:
	GenericExecutor(ExpressionState &state_p) : state(state_p) {}

public:
	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteBinary(Vector &a, Vector &b, Vector &result, idx_t count, FUNC fun) {
		typename A_TYPE::STRUCT_STATE a_state;
		typename B_TYPE::STRUCT_STATE b_state;
		a_state.PrepareVector(a, count);
		b_state.PrepareVector(b, count);

		for (idx_t i = 0; i < count; i++) {
			if (!duckdb_validity_row_is_valid(a_state.validity, i) || !duckdb_validity_row_is_valid(b_state.validity, i)) {
				result.SetRowInvalid(i);
				continue;
			}
			A_TYPE::ARG_TYPE a_val;
			B_TYPE::ARG_TYPE b_val;
			if (!A_TYPE::ConstructType(a_state, i, a_val) || !B_TYPE::ConstructType(b_state, i, b_val)) {
				FlatVector::SetNull(result, i, true);
				continue;
			}
			RESULT_TYPE::AssignResult(result, i, fun(a_val, b_val));
		}
	}
	template <class A_TYPE, class B_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteBinary(DataChunk &args, Vector &result, FUNC fun) {
		ExecuteBinary<A_TYPE, B_TYPE>(args.GetData(0), args.GetData(1), result, args.size(), fun);
	}

private:
	ExpressionState &state;
};

}
