#include "duckdb/parser/base_expression.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

void BaseExpression::Print() const {
	Printer::Print(ToString());
}

string BaseExpression::GetName() const {
	auto name = GetExplicitName();
	if (!name.empty()) {
		return name;
	}
	return ToString();
}

string BaseExpression::GetExplicitName() const {
#ifdef DEBUG
	if (DBConfigOptions::debug_print_bindings) {
		return string();
	}
#endif
	return alias;
}

bool BaseExpression::Equals(const BaseExpression &other) const {
	if (expression_class != other.expression_class || type != other.type) {
		return false;
	}
	return true;
}

void BaseExpression::Verify() const {
}

} // namespace duckdb
