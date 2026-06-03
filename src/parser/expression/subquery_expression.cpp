#include "duckdb/parser/expression/subquery_expression.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

SubqueryExpression::SubqueryExpression()
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery_type(SubqueryType::INVALID),
      comparison_type(ExpressionType::INVALID) {
}

SubqueryExpression::SubqueryExpression(SubqueryType subquery_type_p, unique_ptr<SelectStatement> subquery_p,
                                       unique_ptr<ParsedExpression> child_p, ExpressionType comparison_type_p)
    : ParsedExpression(ExpressionType::SUBQUERY, ExpressionClass::SUBQUERY), subquery(std::move(subquery_p)),
      subquery_type(subquery_type_p), child(std::move(child_p)), comparison_type(comparison_type_p) {
}

string SubqueryExpression::ToString() const {
	switch (subquery_type) {
	case SubqueryType::ANY:
		return "(" + child->ToString() + " " + ExpressionTypeToOperator(comparison_type) + " ANY(" +
		       subquery->ToString() + "))";
	case SubqueryType::EXISTS:
		return "EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::NOT_EXISTS:
		return "NOT EXISTS(" + subquery->ToString() + ")";
	case SubqueryType::SCALAR:
		return "(" + subquery->ToString() + ")";
	default:
		throw InternalException("Unrecognized type for subquery");
	}
}

} // namespace duckdb
