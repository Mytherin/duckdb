#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"

namespace duckdb {

string CTENode::ToString() const {
	return CTEToString(*this);
}

string CTENode::CTEToString(const QueryNode &node) {
	string result;
	// find the
	bool has_recursive = false;
	const_reference<QueryNode> current_node(node);
	while (true) {
		if (current_node.get().type != QueryNodeType::CTE_NODE) {
			// not a CTE anymore
			break;
		}
		auto &cte_node = current_node.get().Cast<CTENode>();
		if (!result.empty()) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(cte_node.ctename);
		if (!cte_node.aliases.empty()) {
			result += " (";
			for (idx_t k = 0; k < cte_node.aliases.size(); k++) {
				if (k > 0) {
					result += ", ";
				}
				result += KeywordHelper::WriteOptionallyQuoted(cte_node.aliases[k]);
			}
			result += ")";
		}
		if (cte_node.query->type == QueryNodeType::RECURSIVE_CTE_NODE) {
			auto &rec_cte = cte_node.query->Cast<RecursiveCTENode>();
			has_recursive = true;
			if (!rec_cte.key_targets.empty()) {
				result += " USING KEY (";
				for (idx_t k = 0; k < rec_cte.key_targets.size(); k++) {
					if (k > 0) {
						result += ", ";
					}
					result += rec_cte.key_targets[k]->ToString();
				}
				result += ") ";
			}
		}
		if (cte_node.materialized == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			result += " AS MATERIALIZED (";
		} else if (cte_node.materialized == CTEMaterialize::CTE_MATERIALIZE_NEVER) {
			result += " AS NOT MATERIALIZED (";
		} else {
			result += " AS (";
		}
		result += cte_node.query->ToString();
		result += ")";
		current_node = *cte_node.child;
	}
	result = "WITH " + ((has_recursive ? "RECURSIVE " : "") + result) + current_node.get().ToString();
	return result;
}

bool CTENode::Equals(const QueryNode *other_p) const {
	if (!QueryNode::Equals(other_p)) {
		return false;
	}
	if (this == other_p) {
		return true;
	}
	auto &other = other_p->Cast<CTENode>();

	if (!query->Equals(other.query.get())) {
		return false;
	}
	if (!child->Equals(other.child.get())) {
		return false;
	}
	return true;
}

unique_ptr<QueryNode> CTENode::Copy() const {
	auto result = make_uniq<CTENode>();
	result->ctename = ctename;
	result->query = query->Copy();
	result->child = child->Copy();
	result->aliases = aliases;
	result->materialized = materialized;
	this->CopyProperties(*result);
	return std::move(result);
}

unique_ptr<QueryNode> CTENode::SerializeCTEChild(Serializer &serializer) const {
	if (serializer.ShouldSerialize(7)) {
		// we can directly serialize the child for newer storage versions
		// FIXME: this copy is wasteful
		return child->Copy();
	}
	// for older storage versions we need to add this CTE node to the cte_map
	auto child_copy = child->Copy();
	reference<QueryNode> current(*child_copy);
	while (current.get().type == QueryNodeType::CTE_NODE) {
		auto &cte_node = current.get().Cast<CTENode>();
		current = *cte_node.child;
	}
	auto cte_info = make_uniq<CommonTableExpressionInfo>();
	cte_info->aliases = aliases;
	auto select_stmt = make_uniq<SelectStatement>();
	select_stmt->node = query->Copy();
	cte_info->query = std::move(select_stmt);
	cte_info->materialized = materialized;
	current.get().LegacyInsertCTE(ctename, std::move(cte_info));
	return child_copy;
}

} // namespace duckdb
