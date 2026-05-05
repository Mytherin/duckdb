//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/expression/bound_comparison_expression.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"

namespace duckdb {

class BoundComparisonExpression : public Expression {
public:
	static constexpr const ExpressionClass TYPE = ExpressionClass::BOUND_COMPARISON;

public:
	BoundComparisonExpression(ExpressionType type, unique_ptr<Expression> left, unique_ptr<Expression> right);

public:
	const Expression &Left() const {
		return *left;
	}
	const Expression &Right() const {
		return *right;
	}
	const unique_ptr<Expression> &LeftRef() const {
		return left;
	}
	const unique_ptr<Expression> &RightRef() const {
		return right;
	}
	unique_ptr<Expression> &LeftMutable() {
		return left;
	}
	unique_ptr<Expression> &RightMutable() {
		return right;
	}
	string ToString() const override;

	bool Equals(const BaseExpression &other) const override;

	unique_ptr<Expression> Copy() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<Expression> Deserialize(Deserializer &deserializer);

public:
	static LogicalType BindComparison(ClientContext &context, const LogicalType &left_type,
	                                  const LogicalType &right_type, ExpressionType comparison_type);
	static bool TryBindComparison(ClientContext &context, const LogicalType &left_type, const LogicalType &right_type,
	                              LogicalType &result_type, ExpressionType comparison_type);

private:
	unique_ptr<Expression> left;
	unique_ptr<Expression> right;
};
} // namespace duckdb
