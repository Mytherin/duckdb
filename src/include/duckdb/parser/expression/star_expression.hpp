//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/expression/star_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/qualified_name_set.hpp"

namespace duckdb {

enum class StarExpressionType : uint8_t { STAR, COLUMNS, UNPACKED, NONE };

//! Represents a * expression in the SELECT clause
class StarExpression : public ParsedExpression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::STAR;

public:
	explicit StarExpression(Identifier relation_name = Identifier());

public:
	const Identifier &RelationName() const {
		return relation_name;
	}
	Identifier &RelationNameMutable() {
		return relation_name;
	}
	const qualified_column_set_t &ExcludeList() const {
		return exclude_list;
	}
	qualified_column_set_t &ExcludeListMutable() {
		return exclude_list;
	}
	const IdentifierMap<unique_ptr<ParsedExpression>> &ReplaceList() const {
		return replace_list;
	}
	IdentifierMap<unique_ptr<ParsedExpression>> &ReplaceListMutable() {
		return replace_list;
	}
	const qualified_column_map_t<Identifier> &RenameList() const {
		return rename_list;
	}
	qualified_column_map_t<Identifier> &RenameListMutable() {
		return rename_list;
	}
	const unique_ptr<ParsedExpression> &Expression() const {
		return expr;
	}
	unique_ptr<ParsedExpression> &ExpressionMutable() {
		return expr;
	}
	bool IsColumns() const {
		return columns;
	}
	bool &IsColumnsMutable() {
		return columns;
	}

	string ToString() const override;

	bool Equals(const ParsedExpression &other) const override;
	hash_t Hash() const override;
	static bool IsStar(const ParsedExpression &a);
	static bool IsColumns(const ParsedExpression &a);
	static bool IsColumnsUnpacked(const ParsedExpression &a);

	unique_ptr<ParsedExpression> Copy() const override;

	static unique_ptr<ParsedExpression>
	DeserializeStarExpression(Identifier &&relation_name, const IdentifierSet &exclude_list,
	                          IdentifierMap<unique_ptr<ParsedExpression>> &&replace_list, bool columns,
	                          unique_ptr<ParsedExpression> expr, bool unpacked,
	                          const qualified_column_set_t &qualified_exclude_list,
	                          qualified_column_map_t<Identifier> &&rename_list);
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParsedExpression> Deserialize(Deserializer &deserializer);

private:
	//! The relation name in case of tbl.*, or empty if this is a normal *
	Identifier relation_name;
	//! List of columns to exclude from the STAR expression
	qualified_column_set_t exclude_list;
	//! List of columns to replace with another expression
	IdentifierMap<unique_ptr<ParsedExpression>> replace_list {IdentifierConstructor::NO_CLIENT_CONTEXT};
	//! List of columns to rename
	qualified_column_map_t<Identifier> rename_list;
	//! The expression to select the columns (regular expression or list)
	unique_ptr<ParsedExpression> expr;
	//! Whether or not this is a COLUMNS expression
	bool columns = false;

public:
	// these methods exist for backwards compatibility of (de)serialization
	StarExpression(const IdentifierSet &exclude_list, qualified_column_set_t qualified_set);

	IdentifierSet SerializedExcludeList() const;
	qualified_column_set_t SerializedQualifiedExcludeList() const;
};
} // namespace duckdb
