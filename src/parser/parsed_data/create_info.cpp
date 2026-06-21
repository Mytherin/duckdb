#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

CreateInfo::CreateInfo(CatalogType type, Identifier schema, Identifier catalog)
    : ParseInfo(TYPE), type(type), name(std::move(catalog), std::move(schema), Identifier()),
      on_conflict(OnCreateConflict::ERROR_ON_CONFLICT), temporary(false), internal(false) {
}

CreateInfo::CreateInfo(CatalogType type, vector<Identifier> schema_path_p)
    : ParseInfo(TYPE), type(type), name(std::move(schema_path_p), Identifier()),
      on_conflict(OnCreateConflict::ERROR_ON_CONFLICT), temporary(false), internal(false) {
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.name = name;
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
