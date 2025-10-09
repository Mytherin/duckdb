#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

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

} // namespace duckdb
