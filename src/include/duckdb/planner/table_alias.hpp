//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/table_alias.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! TableAlias is an alias of a FROM-clause entry, for use during binding
//! The structure of a table alias is "alias\0schema\0catalog" (schema/catalog are optional and only apply to base
//! tables)
struct TableAlias {
	explicit TableAlias(const string &alias);
	TableAlias(const string &schema, const string &alias);
	TableAlias(const string &catalog, const string &schema, const string &alias);

	string alias;

	bool operator==(const TableAlias &other) const {
		return StringUtil::CIEquals(alias, other.alias);
	}
	bool operator<(const TableAlias &other) const {
		return StringUtil::CILessThan(alias, other.alias);
	}

	//! Convert the alias to string (e.g. "catalog.schema.table" or "alias")
	string ToString() const;
	//! Extract the name or alias of the entry
	string GetAlias() const;
	//! Extract the schema name of the entry (or empty string if unavailable)
	string GetSchemaName() const;
	//! Extract the catalog name of the entry (or empty string if unavailable)
	string GetCatalogName() const;
};

template <typename T>
using table_alias_map_t = map<TableAlias, T>;

} // namespace duckdb
