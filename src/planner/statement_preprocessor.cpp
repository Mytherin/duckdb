#include "duckdb/planner/statement_preprocessor.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/parser/parser.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/parser/statement/multi_statement.hpp"
#include "duckdb/parser/parsed_data/bound_pragma_info.hpp"
#include "duckdb/function/function.hpp"

#include "duckdb/main/client_context.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {

StatementPreprocessor::StatementPreprocessor(ClientContext &context) : context(context) {
}

unique_ptr<SQLStatement> StatementPreprocessor::TryReparsePragma(unique_ptr<SQLStatement> statement) const {
	// Try reparsing
	const auto info = statement->Cast<PragmaStatement>().info->Copy();
	QueryErrorContext error_context(statement->stmt_location);
	const auto binder = Binder::CreateBinder(context);
	const auto bound_info = binder->BindPragma(*info, error_context);
	if (!bound_info->function.query) {
		// No reparsing required
		return statement;
	}
	const FunctionParameters parameters {bound_info->parameters, bound_info->named_parameters};
	const auto query_to_reparse = bound_info->function.query(context, parameters);
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(query_to_reparse);
	auto &reparsed = parser.statements;
	if (reparsed.empty()) {
		// the pragma expanded into nothing - swallow it
		return nullptr;
	}
	if (reparsed.size() == 1) {
		return std::move(reparsed[0]);
	}
	// The pragma expanded into multiple statements - execute them as a single multi-statement so that
	// the entire expansion is executed as one transactional unit. The final statement provides the result.
	auto multi_statement = make_uniq<MultiStatement>();
	multi_statement->query = statement->query;
	multi_statement->stmt_location = statement->stmt_location;
	multi_statement->stmt_length = statement->stmt_length;
	for (auto &stmt : reparsed) {
		// the final statement provides the result
		multi_statement->AddResultStatement(std::move(stmt));
	}
	return std::move(multi_statement);
}

void StatementPreprocessor::Preprocess(ClientContextLock &lock, vector<unique_ptr<SQLStatement>> &statements) {
	// Quick check: do we need preprocessing at all?
	bool needs_preprocessing = false;
	for (auto &stmt : statements) {
		if (stmt->type == StatementType::PRAGMA_STATEMENT) {
			needs_preprocessing = true;
			break;
		}
	}
	if (!needs_preprocessing) {
		return;
	}

	context.RunFunctionInTransactionInternal(lock, [&] { PreprocessInternal(lock, statements); });
}

void StatementPreprocessor::PreprocessInternal(ClientContextLock &lock,
                                               vector<unique_ptr<SQLStatement>> &statements) const {
	vector<unique_ptr<SQLStatement>> new_statements;
	for (auto &statement : statements) {
		if (statement->type != StatementType::PRAGMA_STATEMENT) {
			new_statements.push_back(std::move(statement));
			continue;
		}
		auto reparsed = TryReparsePragma(std::move(statement));
		if (!reparsed) {
			continue;
		}
		new_statements.push_back(std::move(reparsed));
	}
	statements = std::move(new_statements);
}

} // namespace duckdb
