//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/type_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

class TypeExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::TYPE;

	//! Primary constructor: the (optionally qualified) type name as a single QualifiedName
	TypeExpression(QualifiedName name, vector<unique_ptr<ParsedExpression>> children);
	//! Convenience overloads that build the QualifiedName from a separate catalog/schema/name or a bare name
	TypeExpression(Identifier catalog, Identifier schema, Identifier type_name,
	               vector<unique_ptr<ParsedExpression>> children);
	TypeExpression(Identifier type_name, vector<unique_ptr<ParsedExpression>> children);
	TypeExpression(const string &type_name, vector<unique_ptr<ParsedExpression>> children);

public:
	const QualifiedName &GetQualifiedName() const {
		return name;
	}
	void SetQualifiedName(QualifiedName name_p) {
		name = std::move(name_p);
	}
	const Identifier &GetTypeName() const {
		return name.name;
	}
	void SetTypeName(Identifier type_name) {
		name.name = std::move(type_name);
	}
	const Identifier &GetSchema() const {
		return name.GetSchema();
	}
	void SetSchema(Identifier new_schema) {
		name.SetSchema(std::move(new_schema));
	}
	const Identifier &GetCatalog() const {
		return name.GetCatalog();
	}
	void SetCatalog(Identifier new_catalog) {
		name.SetCatalog(std::move(new_catalog));
	}
	const vector<Identifier> &GetSchemaPath() const {
		return name.GetSchemaPath();
	}
	void SetSchemaPath(vector<Identifier> path) {
		name.SetSchemaPath(std::move(path));
	}
	const vector<unique_ptr<ParsedExpression>> &GetChildren() const {
		return children;
	}
	vector<unique_ptr<ParsedExpression>> &GetChildren() {
		return children;
	}

public:
	string ToString() const override;

	unique_ptr<ParsedExpression> Copy() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

	void Verify() const override;

private:
	TypeExpression();

	//! The (optionally qualified) type name - the bare name plus its catalog/schema qualification path
	QualifiedName name;

	//! Children of the type expression (e.g. type parameters)
	vector<unique_ptr<ParsedExpression>> children;
};

} // namespace duckdb
