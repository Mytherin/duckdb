#include "duckdb/planner/table_alias.hpp"

namespace duckdb {

TableAlias::TableAlias(const string &alias_p) {
	this->alias = alias_p;
}

TableAlias::TableAlias(const string &schema, const string &alias_p) {
	this->alias = alias_p + '\0' + schema;
}

TableAlias::TableAlias(const string &catalog, const string &schema, const string &alias_p) {
	this->alias = alias_p + '\0' + schema + '\0' + catalog;
}

string TableAlias::ToString() const {
	auto catalog = GetCatalogName();
	auto schema = GetSchemaName();
	auto alias_name = GetAlias();
	string result;
	if (!catalog.empty()) {
		result = catalog + ".";
	}
	if (!schema.empty()) {
		result += schema + ".";
	}
	D_ASSERT(!alias_name.empty());
	result += alias_name;
	return result;
}

string TableAlias::GetAlias() const {
	for (idx_t i = 0; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			return alias.substr(0, i);
		}
	}
	return alias;
}

string TableAlias::GetSchemaName() const {
	idx_t i;
	for (i = 0; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			break;
		}
	}
	if (i >= alias.size()) {
		// no schema name
		return string();
	}
	idx_t start = ++i;
	for (; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			return alias.substr(start, i - start);
		}
	}
	return alias.substr(start);
}

string TableAlias::GetCatalogName() const {
	idx_t i;
	for (i = 0; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			break;
		}
	}
	if (i >= alias.size()) {
		// no schema name
		return string();
	}
	for (i++; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			break;
		}
	}
	if (i >= alias.size()) {
		// no catalog name
		return string();
	}
	idx_t start = ++i;
	for (; i < alias.size(); i++) {
		if (alias[i] == '\0') {
			return alias.substr(start, i - start);
		}
	}
	return alias.substr(start);
}

} // namespace duckdb
