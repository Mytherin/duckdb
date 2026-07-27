//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/statement_preprocessor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"

namespace duckdb {
class ClientContext;
class ClientContextLock;
class SQLStatement;
struct PragmaInfo;

//! Preprocesses parsed statements: expands pragmas that reparse into other statements
class StatementPreprocessor {
public:
	explicit StatementPreprocessor(ClientContext &context);
	void Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements);
	void PreprocessInternal(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements) const;

private:
	ClientContext &context;

private:
	//! Handles a pragma statement, determines whether the statement needs reparsing. If it does, the pragma is
	//! replaced by the reparsed statement - or by a multi-statement if it expands into multiple statements.
	//! Returns nullptr if the pragma expands into nothing.
	unique_ptr<SQLStatement> TryReparsePragma(unique_ptr<SQLStatement> statement) const;
};
} // namespace duckdb
