#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

using Filter = FilterPushdown::Filter;

static void ReplaceSetOpBindings(vector<ColumnBinding> &bindings, Filter &filter, Expression &expr,
                                 LogicalSetOperation &setop) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &colref = expr.Cast<BoundColumnRefExpression>();
		D_ASSERT(colref.binding.table_index == setop.table_index);
		D_ASSERT(colref.depth == 0);

		// rewrite the binding by looking into the bound_tables list of the subquery
		colref.binding = bindings[colref.binding.column_index];
		filter.bindings.insert(colref.binding.table_index);
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](Expression &child) { ReplaceSetOpBindings(bindings, filter, child, setop); });
}

unique_ptr<LogicalOperator> FilterPushdown::PushdownSetOperation(unique_ptr<LogicalOperator> op) {
	D_ASSERT(op->type == LogicalOperatorType::LOGICAL_UNION || op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	         op->type == LogicalOperatorType::LOGICAL_INTERSECT);
	auto &setop = op->Cast<LogicalSetOperation>();

	for (auto &child : op->children) {
		auto child_bindings = child->GetColumnBindings();

		FilterPushdown child_pushdown(optimizer, convert_mark_joins);
		for (auto &original_filter : filters) {
			// first create a copy of the filter
			auto filter = make_uniq<Filter>();
			filter->filter = original_filter->filter->Copy();

			//  rewrite references to the result of the union into references to the child index
			ReplaceSetOpBindings(child_bindings, *filter, *filter->filter, setop);

			// extract bindings again
			filter->ExtractBindings();

			// move the filters into the child pushdown nodes
			child_pushdown.filters.push_back(std::move(filter));
		}

		// pushdown into the child
		child = child_pushdown.Rewrite(std::move(child));
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		// for UNION we can erase all children that are empty
		for (idx_t i = 0; i < op->children.size(); i++) {
			if (op->children[i]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
				op->children.erase(op->children.begin() + static_cast<int64_t>(i));
				i--;
			}
		}
		switch (op->children.size()) {
		case 0:
			// no children left
			return make_uniq<LogicalEmptyResult>(std::move(op));
		case 1: {
			// single child left
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				// union or except with empty right child: return left child
				auto &projection = op->children[0]->Cast<LogicalProjection>();
				projection.table_index = setop.table_index;
				return std::move(op->children[0]);
			}
			return op;
		}
		default:
			// return the operator
			return op;
		}
	}
	bool left_empty = op->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	bool right_empty = op->children[1]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT;
	if (left_empty && right_empty) {
		// both empty: return empty result
		return make_uniq<LogicalEmptyResult>(std::move(op));
	}
	if (left_empty && setop.setop_all) {
		// left child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_EXCEPT:
			// except: if left child is empty, return empty result
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	} else if (right_empty && setop.setop_all) {
		// right child is empty result
		switch (op->type) {
		case LogicalOperatorType::LOGICAL_EXCEPT:
			if (op->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				// union or except with empty right child: return left child
				auto &projection = op->children[0]->Cast<LogicalProjection>();
				projection.table_index = setop.table_index;
				return std::move(op->children[0]);
			}
			break;
		case LogicalOperatorType::LOGICAL_INTERSECT:
			// intersect: if any child is empty, return empty result itself
			return make_uniq<LogicalEmptyResult>(std::move(op));
		default:
			throw InternalException("Unsupported set operation");
		}
	}
	return op;
}

} // namespace duckdb
