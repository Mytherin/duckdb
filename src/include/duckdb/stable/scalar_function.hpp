//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/scalar_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/stable/logical_type.hpp"
#include <string>
#include <vector>

namespace duckdb_stable {
class LogicalType;
class DataChunk;
class ExpressionState;
class Vector;

class CScalarFunction {
public:
	CScalarFunction(duckdb_scalar_function function)  : function(function) {
	}
	~CScalarFunction() {
		if (function) {
			duckdb_destroy_scalar_function(&function);
		}
	}
	// disable copy constructors
	CScalarFunction(const CScalarFunction &other) = delete;
	CScalarFunction &operator=(const CScalarFunction &) = delete;
	//! enable move constructors
	CScalarFunction(CScalarFunction &&other) noexcept : function(nullptr) {
		std::swap(function, other.function);
	}
	CScalarFunction &operator=(CScalarFunction &&other) noexcept {
		std::swap(function, other.function);
		return *this;
	}

	duckdb_scalar_function c_function() {
		return function;
	}

private:
	duckdb_scalar_function function;
};

class ScalarFunction {
public:
	virtual const char *Name() const {
		throw std::runtime_error("ScalarFunction does not have a name defined - it can only be added to a set");
	}
	virtual LogicalType ReturnType() const = 0;
	virtual std::vector<LogicalType> Arguments() const = 0;
	virtual duckdb_scalar_function_t GetFunction() const = 0;

	CScalarFunction CreateFunction(const char *name_override = nullptr) {
		auto scalar_function = duckdb_create_scalar_function();
		duckdb_scalar_function_set_name(scalar_function, name_override ? name_override : Name());
		for(auto &arg : Arguments()) {
			duckdb_scalar_function_add_parameter(scalar_function, arg.c_type());
		}
		duckdb_scalar_function_set_return_type(scalar_function, ReturnType().c_type());
		duckdb_scalar_function_set_function(scalar_function, GetFunction());
		return CScalarFunction(scalar_function);
	}
};

class ScalarFunctionSet {
public:
	ScalarFunctionSet(const char *name_p) : name(name_p) {
		set = duckdb_create_scalar_function_set(name_p);
	}
	~ScalarFunctionSet() {
		if (set) {
			duckdb_destroy_scalar_function_set(&set);
		}
	}

	void AddFunction(ScalarFunction &function) {
		auto scalar_function = function.CreateFunction(name.c_str());
		duckdb_add_scalar_function_to_set(set, scalar_function.c_function());
	}

	duckdb_scalar_function_set c_set() {
		return set;
	}

private:
	std::string name;
	duckdb_scalar_function_set set;
};

}
