//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/tableref/basetableref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/table_description.hpp"
#include "duckdb/parser/tableref.hpp"
#include "duckdb/parser/tableref/at_clause.hpp"
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

//! Represents a TableReference to a base table in a catalog and schema.
class BaseTableRef : public TableRef {
public:
	static constexpr const TableReferenceType TYPE = TableReferenceType::BASE_TABLE;

public:
	BaseTableRef() : TableRef(TableReferenceType::BASE_TABLE) {
	}
	explicit BaseTableRef(const TableDescription &description)
	    : TableRef(TableReferenceType::BASE_TABLE), name(description.database, description.schema, description.table) {
	}

	//! The (optionally qualified) table name - the bare table name plus its catalog/schema qualification path
	QualifiedName name;
	//! The timestamp/version at which to read this table entry (if any)
	unique_ptr<AtClause> at_clause;

public:
	const Identifier &GetCatalogName() const {
		return name.GetCatalog();
	}
	const Identifier &GetSchemaName() const {
		return name.GetSchema();
	}
	const Identifier &GetTableName() const {
		return name.name;
	}
	void SetQualifiedName(QualifiedName name_p) {
		name = std::move(name_p);
	}

public:
	string ToString() const override;
	bool Equals(const TableRef &other_p) const override;
	unique_ptr<TableRef> Copy() override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<TableRef> Deserialize(Deserializer &source);
};

} // namespace duckdb
