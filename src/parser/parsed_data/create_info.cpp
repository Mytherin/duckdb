#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

CreateInfo::CreateInfo(CatalogType type, Identifier schema, Identifier catalog)
    : ParseInfo(TYPE), type(type), on_conflict(OnCreateConflict::ERROR_ON_CONFLICT), temporary(false), internal(false) {
	if (!catalog.empty()) {
		// fully qualified - the path holds [catalog, schema]
		schema_path.push_back(std::move(catalog));
		schema_path.push_back(std::move(schema));
	} else if (!schema.empty()) {
		schema_path.push_back(std::move(schema));
	}
}

void CreateInfo::SetCatalog(Identifier catalog_p) {
	auto schema = GetSchema();
	schema_path.clear();
	if (!catalog_p.empty()) {
		schema_path.push_back(std::move(catalog_p));
		schema_path.push_back(std::move(schema));
	} else if (!schema.empty()) {
		schema_path.push_back(std::move(schema));
	}
}

void CreateInfo::SetSchema(Identifier schema_p) {
	auto catalog = GetCatalog();
	schema_path.clear();
	if (!catalog.empty()) {
		schema_path.push_back(std::move(catalog));
		schema_path.push_back(std::move(schema_p));
	} else if (!schema_p.empty()) {
		schema_path.push_back(std::move(schema_p));
	}
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.schema_path = schema_path;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.extension_name = extension_name;
	other.sql = sql;
	other.dependencies = dependencies;
	other.comment = comment;
	other.tags = tags;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

string CreateInfo::GetCreatePrefix(const string &entry) const {
	string prefix = "CREATE";
	if (on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		prefix += " OR REPLACE";
	}
	if (temporary) {
		prefix += " TEMP";
	}
	prefix += " " + entry + " ";

	if (on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		prefix += " IF NOT EXISTS ";
	}
	return prefix;
}

} // namespace duckdb
