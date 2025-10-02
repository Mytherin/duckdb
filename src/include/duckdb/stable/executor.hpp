//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/generic_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/executor_types.hpp"
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

class Executor {
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

class CastExecutor : public Executor {
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

class FunctionExecutor : public Executor {
public:
	FunctionExecutor(duckdb_function_info info_p) : info(info_p) {
	}

public:
	bool Success() override {
		return true;
	}

	duckdb_function_info c_info() {
		return info;
	}

protected:
	bool SetError(const char *error_message, idx_t r, Vector &result) override {
		throw std::runtime_error(error_message);
	}

private:
	duckdb_function_info info;
};
} // namespace duckdb_stable
