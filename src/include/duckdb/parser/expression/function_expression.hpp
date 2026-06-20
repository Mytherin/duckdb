//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/function_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/result_modifier.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

// An argument passed to a function, consisting of an optional name and an expression.
// The name is used for named arguments, and the expression is the value of the argument.
class FunctionArgument {
public:
	// NOLINTNEXTLINE (allow implicit conversions)
	FunctionArgument(unique_ptr<ParsedExpression> expression_p) : expression(std::move(expression_p)) {
	}

	FunctionArgument(Identifier name_p, unique_ptr<ParsedExpression> expression_p)
	    : name(std::move(name_p)), expression(std::move(expression_p)) {
	}

	const Identifier &GetName() const {
		return name;
	}

	bool HasName() const {
		return !name.empty();
	}

	unique_ptr<ParsedExpression> &GetExpressionMutable() {
		return expression;
	}
	const ParsedExpression &GetExpression() const {
		return *expression;
	}

	FunctionArgument Copy() const {
		return FunctionArgument(name, expression ? expression->Copy() : nullptr);
	}

	bool Equals(const FunctionArgument &other) const {
		if (name != other.name) {
			return false;
		}
		if (expression && other.expression) {
			return expression->Equals(*other.expression);
		}
		return !expression && !other.expression;
	}

	hash_t Hash() const;

	string ToString() const {
		if (name.empty()) {
			return expression->ToString();
		}
		return StringUtil::Format("%s := %s", SQLIdentifier(name), expression->ToString());
	}

	void Serialize(Serializer &serializer) const;
	static FunctionArgument Deserialize(Deserializer &deserializer);

private:
	Identifier name;
	unique_ptr<ParsedExpression> expression;
};

//! Represents a function call
class FunctionExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::FUNCTION;

public:
	DUCKDB_API FunctionExpression(Identifier catalog_name, Identifier schema_name, const Identifier &function_name,
	                              vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);
	DUCKDB_API FunctionExpression(const Identifier &function_name, vector<unique_ptr<ParsedExpression>> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);
	DUCKDB_API FunctionExpression(Identifier catalog_name, Identifier schema_name, const Identifier &function_name,
	                              vector<FunctionArgument> children, unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);
	DUCKDB_API FunctionExpression(const Identifier &function_name, vector<FunctionArgument> children,
	                              unique_ptr<ParsedExpression> filter = nullptr,
	                              unique_ptr<OrderModifier> order_bys = nullptr, bool distinct = false,
	                              bool is_operator = false, bool export_state = false);

public:
	//! The catalog is only set when the function is fully qualified, i.e. schema_path holds [catalog, schema]
	const Identifier &Catalog() const {
		return schema_path.size() >= 2 ? schema_path[0] : Identifier::Empty();
	}
	//! The schema is the last element of the qualification path (empty if the path is empty)
	const Identifier &Schema() const {
		if (schema_path.size() == 1) {
			return schema_path[0];
		}
		if (schema_path.size() >= 2) {
			return schema_path[1];
		}
		return Identifier::Empty();
	}
	void SetCatalog(Identifier catalog_p);
	void SetSchema(Identifier schema_p);
	const vector<Identifier> &GetSchemaPath() const {
		return schema_path;
	}
	void SetSchemaPath(vector<Identifier> path) {
		schema_path = std::move(path);
	}
	const Identifier &FunctionName() const {
		return function_name;
	}
	Identifier &FunctionNameMutable() {
		return function_name;
	}
	void SetFunctionName(string function_name_p) {
		function_name = Identifier(std::move(function_name_p));
	}
	bool IsOperator() const {
		return is_operator;
	}
	bool &IsOperatorMutable() {
		return is_operator;
	}
	bool Distinct() const {
		return distinct;
	}
	bool &DistinctMutable() {
		return distinct;
	}
	const unique_ptr<ParsedExpression> &Filter() const {
		return filter;
	}
	unique_ptr<ParsedExpression> &FilterMutable() {
		return filter;
	}
	const unique_ptr<OrderModifier> &OrderBy() const {
		return order_bys;
	}
	unique_ptr<OrderModifier> &OrderByMutable() {
		return order_bys;
	}
	bool ExportState() const {
		return export_state;
	}
	bool &ExportStateMutable() {
		return export_state;
	}

	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

	//! Returns a pointer to the lambda expression, if the function has a lambda expression as a child, else nullptr.
	optional_ptr<ParsedExpression> IsLambdaFunction();

	bool IsLegacyFunctionCall() const {
		return is_legacy_function_call;
	}

	const vector<FunctionArgument> &GetArguments() const {
		return arguments;
	}
	vector<FunctionArgument> &GetArgumentsMutable() {
		return arguments;
	}

private:
	//! Qualification path: element 0 is the catalog (when present), the remainder are schema levels.
	//! Today this holds at most [catalog, schema]; (catalog, schema) is derived following the size rules above.
	vector<Identifier> schema_path;
	//! Function name
	Identifier function_name;
	//! Whether or not the function is an operator, only used for rendering
	bool is_operator;
	//! List of arguments to the function
	vector<FunctionArgument> arguments;
	//! Whether or not the aggregate function is distinct, only used for aggregates
	bool distinct;
	//! Expression representing a filter, only used for aggregates
	unique_ptr<ParsedExpression> filter;
	//! Modifier representing an ORDER BY, only used for aggregates
	unique_ptr<OrderModifier> order_bys;
	//! whether this function should export its state or not
	bool export_state;

	//! Whether this function is a legacy function call, which means it was parsed from a function call that does not
	//! use the new function argument syntax. This is used to determine how to handle named arguments during binding.
	bool is_legacy_function_call = false;

private:
	FunctionExpression();
};
} // namespace duckdb
