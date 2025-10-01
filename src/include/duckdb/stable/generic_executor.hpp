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
#include "duckdb/stable/string_type.hpp"
#include "duckdb/stable/vector.hpp"
#include <functional>

namespace duckdb_stable {
class ExpressionState;

template <class T>
struct ResultValue {
	ResultValue() = default;
	ResultValue(T val_p) : val(val_p), is_null(false) {
	} // NOLINT: allow implicit conversion
	ResultValue(nullptr_t) : is_null(true) {
	} // NOLINT: allow implicit conversion

	T val;
	bool is_null = false;
};

template <class INPUT_TYPE>
struct PrimitiveTypeState {
	INPUT_TYPE *data = nullptr;
	uint64_t *validity = nullptr;

	void PrepareVector(Vector &input, idx_t count) {
		data = reinterpret_cast<INPUT_TYPE *>(duckdb_vector_get_data(input.c_vec()));
		validity = duckdb_vector_get_validity(input.c_vec());
	}
};

template <class INPUT_TYPE>
struct PrimitiveType {
	PrimitiveType() = default;
	PrimitiveType(INPUT_TYPE val) : val(val) { // NOLINT: allow implicit cast
	}

	INPUT_TYPE val;

	using ARG_TYPE = INPUT_TYPE;
	using STRUCT_STATE = PrimitiveTypeState<INPUT_TYPE>;

	static void ConstructType(STRUCT_STATE &state, idx_t r, ARG_TYPE &output) {
		output = state.data[r];
	}
	static void SetNull(Vector &result, STRUCT_STATE &result_state, idx_t i) {
		if (!result_state.validity) {
			duckdb_vector_ensure_validity_writable(result.c_vec());
			result_state.validity = duckdb_vector_get_validity(result.c_vec());
		}
		duckdb_validity_set_row_invalid(result_state.validity, i);
	}
	static void AssignResult(Vector &result, idx_t r, ARG_TYPE result_val) {
		auto result_data = reinterpret_cast<INPUT_TYPE *>(duckdb_vector_get_data(result.c_vec()));
		result_data[r] = result_val;
	}
};

template <>
void PrimitiveType<string_t>::AssignResult(Vector &result, idx_t r, ARG_TYPE result_val) {
	duckdb_vector_assign_string_element_len(result.c_vec(), r, result_val.GetData(), result_val.GetSize());
}

template <class A_TYPE, class B_TYPE, class C_TYPE>
struct StructTypeStateTernary {
	typename A_TYPE::STRUCT_STATE a_state;
	typename B_TYPE::STRUCT_STATE b_state;
	typename C_TYPE::STRUCT_STATE c_state;
	uint64_t *validity = nullptr;

	void PrepareVector(Vector &input, idx_t count) {
		Vector a_vector(duckdb_struct_vector_get_child(input.c_vec(), 0));
		a_state.PrepareVector(a_vector, count);

		Vector b_vector(duckdb_struct_vector_get_child(input.c_vec(), 1));
		b_state.PrepareVector(b_vector, count);

		Vector c_vector(duckdb_struct_vector_get_child(input.c_vec(), 2));
		c_state.PrepareVector(c_vector, count);

		validity = duckdb_vector_get_validity(input.c_vec());
	}
};

template <class A_TYPE, class B_TYPE, class C_TYPE>
struct StructTypeTernary {
	typename A_TYPE::ARG_TYPE a_val;
	typename B_TYPE::ARG_TYPE b_val;
	typename C_TYPE::ARG_TYPE c_val;

	using ARG_TYPE = StructTypeTernary<A_TYPE, B_TYPE, C_TYPE>;
	using STRUCT_STATE = StructTypeStateTernary<A_TYPE, B_TYPE, C_TYPE>;

	static void ConstructType(STRUCT_STATE &state, idx_t r, ARG_TYPE &output) {
		A_TYPE::ConstructType(state.a_state, r, output.a_val);
		B_TYPE::ConstructType(state.b_state, r, output.b_val);
		C_TYPE::ConstructType(state.c_state, r, output.c_val);
	}
	static void SetNull(Vector &result, STRUCT_STATE &result_state, idx_t r) {
		if (!result_state.validity) {
			duckdb_vector_ensure_validity_writable(result.c_vec());
			result_state.validity = duckdb_vector_get_validity(result.c_vec());
		}
		duckdb_validity_set_row_invalid(result_state.validity, r);

		auto a_child = result.GetChild(0);
		auto b_child = result.GetChild(1);
		auto c_child = result.GetChild(2);
		A_TYPE::SetNull(a_child, result_state.a_state, r);
		B_TYPE::SetNull(b_child, result_state.b_state, r);
		C_TYPE::SetNull(c_child, result_state.c_state, r);
	}
	static void AssignResult(Vector &result, idx_t r, ARG_TYPE result_val) {
		auto a_child = result.GetChild(0);
		A_TYPE::AssignResult(a_child, r, result_val.a_val);

		auto b_child = result.GetChild(1);
		B_TYPE::AssignResult(b_child, r, result_val.b_val);

		auto c_child = result.GetChild(2);
		C_TYPE::AssignResult(c_child, r, result_val.c_val);
	}
};

class GenericExecutor {
public:
	template <class A_TYPE, class RESULT_TYPE, class FUNC>
	void ExecuteUnary(Vector &input, Vector &result, idx_t count, FUNC fun) {
		typename A_TYPE::STRUCT_STATE a_state;
		a_state.PrepareVector(input, count);

		typename RESULT_TYPE::STRUCT_STATE result_state;
		for (idx_t r = 0; r < count; r++) {
			if (!duckdb_validity_row_is_valid(a_state.validity, r)) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			typename A_TYPE::ARG_TYPE a_val;
			A_TYPE::ConstructType(a_state, r, a_val);

			ResultValue<typename RESULT_TYPE::ARG_TYPE> result_value;
			try {
				result_value = fun(a_val);
			} catch (std::exception &ex) {
				if (!SetError(ex.what(), r, result)) {
					return;
				}
				continue;
			}
			if (result_value.is_null) {
				RESULT_TYPE::SetNull(result, result_state, r);
				continue;
			}
			RESULT_TYPE::AssignResult(result, r, result_value.val);
		}
	}

	virtual bool Success() {
		return true;
	}

protected:
	virtual bool SetError(const char *error_message, idx_t r, Vector &result) = 0;
};

class CastExecutor : public GenericExecutor {
public:
	CastExecutor(duckdb_function_info info_p) : info(info_p) {
		cast_mode = duckdb_cast_function_get_cast_mode(info);
	}

public:
	bool Success() override {
		return success;
	}

protected:
	bool SetError(const char *error_message, idx_t r, Vector &result) override {
		duckdb_cast_function_set_row_error(info, error_message, r, result.c_vec());
		if (cast_mode == DUCKDB_CAST_TRY) {
			return true;
		}
		success = false;
		return false;
	}

private:
	duckdb_function_info info;
	duckdb_cast_mode cast_mode;
	bool success = true;
};

} // namespace duckdb_stable
