#include "duckdb/parser/parsed_data/alter_info.hpp"

#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

AlterInfo::AlterInfo(AlterType type, Identifier catalog, Identifier schema, Identifier name_p,
                     OnEntryNotFound if_not_found)
    : ParseInfo(TYPE), type(type), if_not_found(if_not_found),
      name(std::move(catalog), std::move(schema), std::move(name_p)), allow_internal(false) {
}

AlterInfo::AlterInfo(AlterType type, vector<Identifier> schema_path_p, Identifier name_p, OnEntryNotFound if_not_found)
    : ParseInfo(TYPE), type(type), if_not_found(if_not_found), name(std::move(schema_path_p), std::move(name_p)),
      allow_internal(false) {
}

AlterInfo::AlterInfo(AlterType type) : ParseInfo(TYPE), type(type) {
}

AlterInfo::~AlterInfo() {
}

AlterEntryData AlterInfo::GetAlterEntryData() const {
	AlterEntryData data;
	data.catalog = GetCatalog();
	data.schema = GetSchema();
	data.name = name.name;
	data.if_not_found = if_not_found;
	return data;
}

bool AlterInfo::IsAddPrimaryKey() const {
	if (type != AlterType::ALTER_TABLE) {
		return false;
	}

	auto &table_info = Cast<AlterTableInfo>();
	if (table_info.alter_table_type != AlterTableType::ADD_CONSTRAINT) {
		return false;
	}

	auto &constraint_info = table_info.Cast<AddConstraintInfo>();
	if (constraint_info.constraint->type != ConstraintType::UNIQUE) {
		return false;
	}

	auto &unique_info = constraint_info.constraint->Cast<UniqueConstraint>();
	if (!unique_info.IsPrimaryKey()) {
		return false;
	}

	return true;
}

} // namespace duckdb
