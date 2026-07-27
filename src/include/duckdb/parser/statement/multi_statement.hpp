//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/statement/multi_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace duckdb {

class MultiStatement : public SQLStatement {
public:
	static constexpr const StatementType TYPE = StatementType::MULTI_STATEMENT;

public:
	MultiStatement();

	//! The statements that make up this multi-statement - executed in-order as a single unit
	vector<unique_ptr<SQLStatement>> statements;

protected:
	MultiStatement(const MultiStatement &other);

public:
	//! Add a statement to the multi-statement, and make it the statement that provides the result
	void AddResultStatement(unique_ptr<SQLStatement> statement);
	//! Add a statement to the multi-statement without altering which statement provides the result
	void AddStatement(unique_ptr<SQLStatement> statement);
	//! Make the statement at the given index the statement that provides the result
	void SetResultStatementIndex(idx_t index);
	//! The index of the statement whose result is returned to the client
	idx_t ResultStatementIndex() const {
		return result_statement_idx;
	}

	unique_ptr<SQLStatement> Copy() const override;
	string ToString() const override;

private:
	//! The index into `statements` of the statement whose result is returned to the client
	idx_t result_statement_idx = 0;
};

} // namespace duckdb
