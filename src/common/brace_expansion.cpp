#include "duckdb/common/brace_expansion.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {

// see https://www.gnu.org/software/bash/manual/html_node/Brace-Expansion.html for documentation

// A correctly-formed brace expansion must contain unquoted opening and closing braces, and at least one unquoted comma
// or a valid sequence expression. Any incorrectly formed brace expansion is left unchanged.
struct BraceExpression {
	virtual ~BraceExpression() {}
};

// Denotes a comma-delimited list of options
struct BraceExpressionOr : BraceExpression {
	vector<unique_ptr<BraceExpression>> children;
};

struct BraceExpressionList : BraceExpression {
	vector<unique_ptr<BraceExpression>> children;
};

//! Denotes a literal
struct BraceExpressionLiteral : BraceExpression {
	string literal;
};

//! Denotes a sequence
struct BraceExpressionSequence : BraceExpression {
	uint32_t start;
	uint32_t end;
	int32_t increment;
};

static uint32_t ParseCodepoint(const string &pattern, idx_t &pos) {
	int sz;
	auto res = Utf8Proc::UTF8ToCodepoint(pattern.c_str() + pos, sz);
	pos += sz;
	return res;
}

static bool ParseDotDot(const string &pattern, idx_t &pos) {
	// there needs to be two dots after the pattern
	if (pos + 1 >= pattern.size() || pattern[pos] != '.' || pattern[pos + 1] != '.') {
		return false;
	}
	return true;
}

unique_ptr<BraceExpression> TryParseBraceSequence(const string &pattern, idx_t &pos) {
	// sequences have the form of "x..y[..increment]"
	// x and y must be unicode codepoints, increment must be a (signed) number
	// parse the first character
	idx_t current_pos = pos;
	uint32_t start_codepoint = ParseCodepoint(pattern, current_pos);
	if (!ParseDotDot(pattern, current_pos)) {
		return nullptr;
	}
	if (current_pos >= pattern.size()) {
		return nullptr;
	}
	uint32_t end_codepoint = ParseCodepoint(pattern, current_pos);
	// parse either the final bracket, or the increment
	if (current_pos >= pattern.size()) {
		return nullptr;
	}
	if (pattern[current_pos] != '.' && pattern[current_pos] != '}') {
		return nullptr;
	}
	int32_t increment;
	if (pattern[current_pos] == '.') {
		// explicit increment
		if (!ParseDotDot(pattern, current_pos)) {
			return nullptr;
		}
		throw InternalException("FIXME: parse explicit increment");
	} else {
		increment = end_codepoint > start_codepoint ? 1 : -1;
	}
	auto seq = make_uniq<BraceExpressionSequence>();
	seq->start = start_codepoint;
	seq->end = end_codepoint;
	seq->increment = increment;
	pos = current_pos;
	return std::move(seq);
}

unique_ptr<BraceExpression> TryParseBraceExpression(const string &pattern, idx_t &pos) {
	// brace expressions can either be a:
	// (1) sequence
	// (2) list of brace expressions
	// try to parse either of these
	auto seq = TryParseBraceSequence(pattern, pos);
	if (seq) {
		return std::move(seq);
	}
	// list of brace expressions

	bool escape = false;
	idx_t end_pos = pattern.size();
	idx_t current_level = 0;
	for(idx_t i = start; i < pattern.size(); i++) {
		if (escape) {
			escape = false;
			continue;
		}
		if (pattern[i] == '\\') {
			escape = true;
			break;
		}
		if (pattern[i] == '{') {
			// parse the inner level
			TryParseBraceExpression(pattern, i);
			continue;
		}
		if (pattern[i] == '}') {
			current_level--;
			if (current_level == 0)
			continue;
		}
		if (pattern[i] == ',') {

		}
	}
}

void BraceExpansion::ExpandBraces(const string &pattern, vector<string> &result) {
	// try to parse all brace expansions within the pattern
	bool escape = false;
	for(idx_t c_idx = 0; c_idx < pattern.size(); c_idx++) {
		if (escape) {
			escape = false;
			continue;
		}
		auto ch = pattern[ch];
		if (ch == '\\') {
			// A { or ‘,’ may be quoted with a backslash to prevent its being considered part of a brace expression.
			escape = true;
			continue;
		}
		if (ch == '{') {
			// try to parse the brace expression
			idx_t pos = c_idx + 1;
			auto brace_expr = TryParseBraceExpression(pattern, pos);
			if (brace_expr) {
				throw InternalException("FIXME: parsed brace expression - and now?");
			}
		}
	}

}


}
