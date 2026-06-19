#include "duckdb/parser/parsed_data/alter_info.hpp"

#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

namespace duckdb {

AlterInfo::AlterInfo(AlterType type, Identifier catalog, Identifier schema, Identifier name_p,
                     OnEntryNotFound if_not_found)
    : ParseInfo(TYPE), type(type), if_not_found(if_not_found), name(std::move(name_p)), allow_internal(false) {
	if (!catalog.empty()) {
		// fully qualified - the path holds [catalog, schema]
		schema_path.push_back(std::move(catalog));
		schema_path.push_back(std::move(schema));
	} else if (!schema.empty()) {
		schema_path.push_back(std::move(schema));
	}
}

AlterInfo::AlterInfo(AlterType type) : ParseInfo(TYPE), type(type) {
}

AlterInfo::~AlterInfo() {
}

void AlterInfo::SetCatalog(Identifier catalog_p) {
	auto schema = GetSchema();
	schema_path.clear();
	if (!catalog_p.empty()) {
		schema_path.push_back(std::move(catalog_p));
		schema_path.push_back(std::move(schema));
	} else if (!schema.empty()) {
		schema_path.push_back(std::move(schema));
	}
}

void AlterInfo::SetSchema(Identifier schema_p) {
	auto catalog = GetCatalog();
	schema_path.clear();
	if (!catalog.empty()) {
		schema_path.push_back(std::move(catalog));
		schema_path.push_back(std::move(schema_p));
	} else if (!schema_p.empty()) {
		schema_path.push_back(std::move(schema_p));
	}
}

AlterEntryData AlterInfo::GetAlterEntryData() const {
	AlterEntryData data;
	data.catalog = GetCatalog();
	data.schema = GetSchema();
	data.name = name;
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
