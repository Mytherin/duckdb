//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_list.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

typedef ScalarFunction (*get_scalar_function_t)();
typedef ScalarFunctionSet (*get_scalar_function_set_t)();
typedef AggregateFunction (*get_aggregate_function_t)();
typedef AggregateFunctionSet (*get_aggregate_function_set_t)();

struct StaticFunctionDefinition {
	const char *name;
	const char *alias_of;
	const char *parameters;
	const char *description;
	const char *example;
	const char *categories;
	get_scalar_function_t get_function;
	get_scalar_function_set_t get_function_set;
	get_aggregate_function_t get_aggregate_function;
	get_aggregate_function_set_t get_aggregate_function_set;
};

class Catalog;
struct CatalogTransaction;

struct FunctionList {
	static const StaticFunctionDefinition *GetInternalFunctionList();
	static void RegisterFunctions(Catalog &catalog, CatalogTransaction transaction);
	static void RegisterExtensionFunctions(ExtensionLoader &db, const StaticFunctionDefinition *functions);
};

} // namespace duckdb
