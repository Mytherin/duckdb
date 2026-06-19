//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/qualified_name.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/planner/binding_alias.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

struct QualifiedName {
	QualifiedName() = default;
	QualifiedName(Identifier catalog, Identifier schema, Identifier name);

public:
	//! The entry name (not part of the catalog/schema qualification pair)
	Identifier name;

public:
	//! The catalog is only set when the name is fully qualified, i.e. schema_path holds [catalog, schema]
	const Identifier &GetCatalog() const {
		return schema_path.size() >= 2 ? schema_path[0] : Identifier::Empty();
	}
	//! The schema is the last element of the qualification path (empty if the path is empty)
	const Identifier &GetSchema() const {
		if (schema_path.size() == 1) {
			return schema_path[0];
		}
		if (schema_path.size() >= 2) {
			return schema_path[1];
		}
		return Identifier::Empty();
	}
	void SetCatalog(Identifier catalog_p);
	void SetSchema(Identifier schema_p);

	const vector<Identifier> &GetSchemaPath() const {
		return schema_path;
	}
	void SetSchemaPath(vector<Identifier> path) {
		schema_path = std::move(path);
	}

	//! Parse the (optional) schema and a name from a string in the format of e.g. "schema"."table"; if there is no dot
	//! the schema will be set to INVALID_SCHEMA
	static QualifiedName Parse(const string &input);
	static vector<Identifier> ParseComponents(const string &input);
	string ToString() const;

private:
	//! Qualification path: element 0 is the catalog (when present), the remainder are schema levels.
	//! Today this holds at most [catalog, schema]; (catalog, schema) is derived following the size rules above.
	vector<Identifier> schema_path;
};

struct QualifiedColumnName {
	QualifiedColumnName();
	QualifiedColumnName(Identifier column_p); // NOLINT: allow implicit conversion from string to column name
	QualifiedColumnName(Identifier table_p, Identifier column_p);
	QualifiedColumnName(const BindingAlias &alias, Identifier column_p);

	Identifier catalog;
	Identifier schema;
	Identifier table;
	Identifier column;

	static QualifiedColumnName Parse(string &input);

	string ToString() const;

	void Serialize(Serializer &serializer) const;
	static QualifiedColumnName Deserialize(Deserializer &deserializer);

	bool IsQualified() const;

	bool operator==(const QualifiedColumnName &rhs) const;
};

} // namespace duckdb
