#include "duckdb/parser/statement/multi_statement.hpp"

namespace duckdb {

MultiStatement::MultiStatement() : SQLStatement(StatementType::MULTI_STATEMENT) {
}

MultiStatement::MultiStatement(const MultiStatement &other)
    : SQLStatement(other), result_statement_idx(other.result_statement_idx) {
	for (auto &stmt : other.statements) {
		statements.push_back(stmt->Copy());
	}
}

void MultiStatement::AddStatement(unique_ptr<SQLStatement> statement) {
	statements.push_back(std::move(statement));
}

void MultiStatement::AddResultStatement(unique_ptr<SQLStatement> statement) {
	result_statement_idx = statements.size();
	statements.push_back(std::move(statement));
}

void MultiStatement::SetResultStatementIndex(idx_t index) {
	if (index >= statements.size()) {
		throw InternalException("MultiStatement result statement index %llu is out of range (%llu statements)", index,
		                        statements.size());
	}
	result_statement_idx = index;
}

unique_ptr<SQLStatement> MultiStatement::Copy() const {
	return unique_ptr<MultiStatement>(new MultiStatement(*this));
}

string MultiStatement::ToString() const {
	vector<string> stringified;
	for (auto &stmt : statements) {
		stringified.push_back(stmt->ToString());
	}
	return StringUtil::Join(stringified, ";") + ";";
}

} // namespace duckdb
