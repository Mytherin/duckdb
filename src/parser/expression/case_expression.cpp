#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

CaseExpression::CaseExpression() : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE) {
}

CaseExpression::CaseExpression(vector<CaseCheck> case_checks_p, unique_ptr<ParsedExpression> else_expr_p)
    : ParsedExpression(ExpressionType::CASE_EXPR, ExpressionClass::CASE), case_checks(std::move(case_checks_p)),
      else_expr(std::move(else_expr_p)) {
}

string CaseExpression::ToString() const {
	return ToString<CaseExpression, ParsedExpression>(*this);
}

} // namespace duckdb
