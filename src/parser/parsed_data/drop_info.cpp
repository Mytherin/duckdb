#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

const Identifier &DropInfo::EmptyIdentifier() {
	static const Identifier EMPTY;
	return EMPTY;
}

DropInfo::DropInfo() : ParseInfo(TYPE), cascade(false) {
}

DropInfo::DropInfo(const DropInfo &info)
    : ParseInfo(info.info_type), type(info.type), name(info.name), if_not_found(info.if_not_found),
      cascade(info.cascade), allow_drop_internal(info.allow_drop_internal),
      extra_drop_info(info.extra_drop_info ? info.extra_drop_info->Copy() : nullptr), schema_path(info.schema_path) {
}

void DropInfo::SetCatalog(Identifier catalog_p) {
	auto schema = GetSchema();
	schema_path.clear();
	if (!catalog_p.empty()) {
		schema_path.push_back(std::move(catalog_p));
		schema_path.push_back(std::move(schema));
	} else if (!schema.empty()) {
		schema_path.push_back(std::move(schema));
	}
}

void DropInfo::SetSchema(Identifier schema_p) {
	auto catalog = GetCatalog();
	schema_path.clear();
	if (!catalog.empty()) {
		schema_path.push_back(std::move(catalog));
		schema_path.push_back(std::move(schema_p));
	} else if (!schema_p.empty()) {
		schema_path.push_back(std::move(schema_p));
	}
}

unique_ptr<DropInfo> DropInfo::Copy() const {
	return make_uniq<DropInfo>(*this);
}

string DropInfo::ToString() const {
	string result = "";
	if (type == CatalogType::PREPARED_STATEMENT) {
		result += "DEALLOCATE PREPARE ";
		result += SQLIdentifier(name);
	} else {
		result += "DROP";
		result += " " + ParseInfo::TypeToString(type);
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			result += " IF EXISTS";
		}
		result += " ";
		result += QualifierToString(GetCatalog(), GetSchema(), name);
		if (type == CatalogType::TRIGGER_ENTRY && extra_drop_info) {
			auto &trigger_info = extra_drop_info->Cast<ExtraDropTriggerInfo>();
			if (trigger_info.base_table) {
				result += " ON ";
				result += trigger_info.base_table->Cast<BaseTableRef>().ToString();
			}
		}
		if (cascade) {
			result += " CASCADE";
		}
	}
	result += ";";
	return result;
}

} // namespace duckdb
