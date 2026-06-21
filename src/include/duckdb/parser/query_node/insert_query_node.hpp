//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/query_node/insert_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/parsed_expression.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
class OnConflictInfo;
class ExpressionListRef;
class SelectStatement;

enum class InsertColumnOrder : uint8_t;

//! InsertQueryNode represents an INSERT DML statement as a QueryNode,
//! enabling serialization and use as a CTE body.
class InsertQueryNode : public QueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::INSERT_QUERY_NODE;

public:
	InsertQueryNode();

	//! The select statement to insert from
	unique_ptr<SelectStatement> select_statement;
	//! Column names to insert into
	vector<Identifier> columns;

	//! The (optionally qualified) table name to insert to - the bare name plus its catalog/schema path
	QualifiedName table;

	//! keep track of optional returningList if statement contains a RETURNING keyword
	vector<unique_ptr<ParsedExpression>> returning_list;

	unique_ptr<OnConflictInfo> on_conflict_info;
	unique_ptr<TableRef> table_ref;

	//! Whether or not this a DEFAULT VALUES
	bool default_values = false;

	//! INSERT BY POSITION or INSERT BY NAME
	InsertColumnOrder column_order;

public:
	const Identifier &GetCatalog() const {
		return table.GetCatalog();
	}
	const Identifier &GetSchema() const {
		return table.GetSchema();
	}
	void SetCatalog(Identifier catalog_p) {
		table.SetCatalog(std::move(catalog_p));
	}
	void SetSchema(Identifier schema_p) {
		table.SetSchema(std::move(schema_p));
	}
	const vector<Identifier> &GetSchemaPath() const {
		return table.GetSchemaPath();
	}
	void SetSchemaPath(vector<Identifier> path) {
		table.SetSchemaPath(std::move(path));
	}
	//! Assign the full qualified table name (used for v2.0+ deserialization)
	void SetTable(QualifiedName table_p) {
		table = std::move(table_p);
	}
	//! Assign only the bare table name (used for legacy deserialization)
	void SetTableName(Identifier table_name) {
		table.name = std::move(table_name);
	}

	string ToString() const override;
	bool Equals(const QueryNode *other) const override;
	unique_ptr<QueryNode> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<QueryNode> Deserialize(Deserializer &deserializer);

	//! If the INSERT statement is inserted DIRECTLY from a values list (i.e. INSERT INTO tbl VALUES (...)) this returns
	//! the expression list Otherwise, this returns NULL
	optional_ptr<ExpressionListRef> GetValuesList() const;
};

} // namespace duckdb
