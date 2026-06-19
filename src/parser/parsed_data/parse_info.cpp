#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

string ParseInfo::TypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return "TABLE";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "FUNCTION";
	case CatalogType::INDEX_ENTRY:
		return "INDEX";
	case CatalogType::SCHEMA_ENTRY:
		return "SCHEMA";
	case CatalogType::TYPE_ENTRY:
		return "TYPE";
	case CatalogType::VIEW_ENTRY:
		return "VIEW";
	case CatalogType::SEQUENCE_ENTRY:
		return "SEQUENCE";
	case CatalogType::MACRO_ENTRY:
		return "MACRO";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "MACRO TABLE";
	case CatalogType::SECRET_ENTRY:
		return "SECRET";
	case CatalogType::TRIGGER_ENTRY:
		return "TRIGGER";
	default:
		throw InternalException("ParseInfo::TypeToString for CatalogType with type: %s not implemented",
		                        EnumUtil::ToString(type));
	}
}

string ParseInfo::QualifierToString(const vector<Identifier> &schema_path, const Identifier &name) {
	string result;
	if (schema_path.size() == 1) {
		// only a schema is set - omit it when it is the implicit default schema
		if (!schema_path[0].empty() && schema_path[0] != DEFAULT_SCHEMA) {
			result += SQLIdentifier(schema_path[0]) + ".";
		}
	} else {
		// fully qualified - render every level of the path
		for (auto &entry : schema_path) {
			if (!entry.empty()) {
				result += SQLIdentifier(entry) + ".";
			}
		}
	}
	result += SQLIdentifier(name);
	return result;
}

string ParseInfo::QualifierToString(const Identifier &catalog, const Identifier &schema, const Identifier &name) {
	vector<Identifier> schema_path;
	if (!catalog.empty()) {
		schema_path.push_back(catalog);
		schema_path.push_back(schema);
	} else if (!schema.empty()) {
		schema_path.push_back(schema);
	}
	return QualifierToString(schema_path, name);
}

} // namespace duckdb
