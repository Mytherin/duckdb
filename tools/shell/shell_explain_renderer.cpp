//===----------------------------------------------------------------------===//
// Pretty renderer for EXPLAIN / EXPLAIN ANALYZE output
//
// When the shell executes a plain EXPLAIN (ANALYZE) statement, the statement is rewritten to produce JSON output
// (see ShellState::SetupPrettyExplain). This renderer parses the JSON plan and renders it as a tree of
// auto-sized boxes. The main pipeline is stacked vertically, children are placed side-by-side at joins
// (falling back to an indented vertical layout if the terminal is not wide enough), and the output is
// colorized using the shell highlighting configuration.
//===----------------------------------------------------------------------===//
#include "shell_explain_renderer.hpp"
#include "shell_highlight.hpp"

#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

#include <algorithm>

namespace duckdb_shell {

using duckdb::MaxValue;
using duckdb::MinValue;
using duckdb_yyjson::yyjson_arr_size;
using duckdb_yyjson::yyjson_doc;
using duckdb_yyjson::yyjson_doc_free;
using duckdb_yyjson::yyjson_doc_get_root;
using duckdb_yyjson::yyjson_get_num;
using duckdb_yyjson::yyjson_get_str;
using duckdb_yyjson::yyjson_is_arr;
using duckdb_yyjson::yyjson_is_num;
using duckdb_yyjson::yyjson_is_obj;
using duckdb_yyjson::yyjson_is_str;
using duckdb_yyjson::yyjson_obj_get;
using duckdb_yyjson::yyjson_read;
using duckdb_yyjson::yyjson_val;

//! How the query plan tree is laid out horizontally:
//!  - LEFT: the root sits at the top-left and the tree fans out down-and-to-the-right (like the old explain plan)
//!  - CENTER: every parent is centered over its children and the whole tree is centered in the terminal
enum class ExplainAlignment { LEFT, CENTER };

//===--------------------------------------------------------------------===//
// Styled text primitives
//===--------------------------------------------------------------------===//
struct ExplainSpan {
	ExplainSpan(string text_p, HighlightElementType type_p) : text(std::move(text_p)), type(type_p) {
	}
	ExplainSpan(string text_p, PrintColor color_p, PrintIntensity intensity_p)
	    : text(std::move(text_p)), type(HighlightElementType::NONE), has_color_override(true), color(color_p),
	      intensity(intensity_p) {
	}

	string text;
	HighlightElementType type;
	//! Explicit color override (used for e.g. heat-colored timings)
	bool has_color_override = false;
	PrintColor color = PrintColor::STANDARD;
	PrintIntensity intensity = PrintIntensity::STANDARD;
};

using ExplainLine = vector<ExplainSpan>;

//! A rectangular block of rendered lines. `spine` is the column at which a parent connects down into this block
//! (the root box's centre).
struct ExplainBlock {
	vector<ExplainLine> lines;
	idx_t width = 0;
	idx_t spine = 0;
};

static idx_t LineWidth(const ExplainLine &line) {
	idx_t width = 0;
	for (auto &span : line) {
		width += ShellState::RenderLength(span.text);
	}
	return width;
}

static void OffsetLine(ExplainLine &line, idx_t offset) {
	if (offset == 0) {
		return;
	}
	line.insert(line.begin(), ExplainSpan(string(offset, ' '), HighlightElementType::NONE));
}

static void PadLine(ExplainLine &line, idx_t width) {
	idx_t current_width = LineWidth(line);
	if (current_width >= width) {
		return;
	}
	line.emplace_back(string(width - current_width, ' '), HighlightElementType::NONE);
}

//! Number of leading (space) render-columns on a line
static idx_t LeadingSpaces(const ExplainLine &line) {
	idx_t count = 0;
	for (auto &span : line) {
		for (char c : span.text) {
			if (c != ' ') {
				return count;
			}
			count++;
		}
	}
	return count;
}

//! Append `src` to `dst` so that the first non-space character of `src` lands at render column `content_col`. The
//! leading whitespace of `src` is dropped (dst is padded to position it instead), so blocks can be placed at an
//! absolute column even when an earlier block on the same line extends past `content_col`'s nominal start.
static void AppendAt(ExplainLine &dst, const ExplainLine &src, idx_t content_col) {
	PadLine(dst, content_col);
	idx_t to_skip = LeadingSpaces(src);
	idx_t skipped = 0;
	for (auto &span : src) {
		if (skipped >= to_skip) {
			dst.push_back(span);
			continue;
		}
		idx_t i = 0;
		while (i < span.text.size() && span.text[i] == ' ' && skipped < to_skip) {
			i++;
			skipped++;
		}
		if (i < span.text.size()) {
			ExplainSpan trimmed = span;
			trimmed.text = span.text.substr(i);
			dst.push_back(std::move(trimmed));
		}
	}
}

//! Returns true if the codepoint at render column "col" equals the given (single-width) UTF8 character.
static bool RenderCharEquals(const ExplainLine &line, idx_t col, const char *expected) {
	idx_t col_offset = 0;
	for (auto &span : line) {
		idx_t span_width = ShellState::RenderLength(span.text);
		if (col >= col_offset + span_width) {
			col_offset += span_width;
			continue;
		}
		idx_t char_idx = 0;
		for (idx_t i = 0; i < span.text.size(); i++) {
			if (ShellState::IsCharacter(span.text[i])) {
				if (char_idx == col - col_offset) {
					idx_t j = i + 1;
					while (j < span.text.size() && !ShellState::IsCharacter(span.text[j])) {
						j++;
					}
					return span.text.compare(i, j - i, expected) == 0;
				}
				char_idx++;
			}
		}
		return false;
	}
	return false;
}

//! Replace the (single-width) character at render column "col" with the given UTF8 character.
//! Used to draw connectors (e.g. "┬") onto box borders - these lines only contain single-width characters.
static void SetLayoutChar(ExplainLine &line, idx_t col, const char *replacement) {
	idx_t col_offset = 0;
	for (auto &span : line) {
		idx_t span_width = ShellState::RenderLength(span.text);
		if (col >= col_offset + span_width) {
			col_offset += span_width;
			continue;
		}
		// find the byte position of the target codepoint within this span
		idx_t char_idx = 0;
		idx_t byte_start = 0;
		idx_t byte_end = span.text.size();
		for (idx_t i = 0; i < span.text.size(); i++) {
			if (ShellState::IsCharacter(span.text[i])) {
				if (char_idx == col - col_offset) {
					byte_start = i;
				} else if (char_idx == col - col_offset + 1) {
					byte_end = i;
					break;
				}
				char_idx++;
			}
		}
		span.text = span.text.substr(0, byte_start) + replacement + span.text.substr(byte_end);
		return;
	}
}

//! Draw the upward connector (┴) onto a child's top border, where its parent connects down into it.
//! Only replaces a plain horizontal dash so the operator name rendered in the border is never corrupted.
static void DrawRoofConnector(ExplainBlock &child, idx_t col) {
	if (!child.lines.empty() && RenderCharEquals(child.lines.front(), col, "─")) {
		SetLayoutChar(child.lines.front(), col, "┴");
	}
}

//===--------------------------------------------------------------------===//
// Plan tree
//===--------------------------------------------------------------------===//
struct ExplainTreeNode {
	string name;
	//! Extra information - each entry is a key with one or more values (multiple values for e.g. projection lists)
	vector<std::pair<string, vector<string>>> details;
	//! Actual row count (EXPLAIN ANALYZE only)
	optional_idx rows;
	//! Estimated cardinality
	optional_idx estimated_rows;
	//! Operator timing in seconds (EXPLAIN ANALYZE only, -1 if not present)
	double timing = -1;
	vector<unique_ptr<ExplainTreeNode>> children;

	//! Cumulative timing of this whole sub-tree (>= 0); used to decide which sub-trees to flatten away
	double subtree_time = 0;
	//! Number of operators in this sub-tree (including this node)
	idx_t subtree_count = 1;
};

//! Fill subtree_time / subtree_count for the whole tree
static void ComputeSubtreeStats(ExplainTreeNode &node) {
	node.subtree_time = node.timing > 0 ? node.timing : 0;
	node.subtree_count = 1;
	for (auto &child : node.children) {
		ComputeSubtreeStats(*child);
		node.subtree_time += child->subtree_time;
		node.subtree_count += child->subtree_count;
	}
}

//! The operator name as shown in a box title - underscores are turned into spaces (e.g. HASH_JOIN -> HASH JOIN)
static string DisplayName(const string &name) {
	string result = name;
	std::replace(result.begin(), result.end(), '_', ' ');
	return result;
}

static HighlightElementType GetOperatorElement(const ExplainTreeNode &node) {
	auto &name = node.name;
	if (StringUtil::Contains(name, "SCAN") || StringUtil::Contains(name, "GET")) {
		return HighlightElementType::EXPLAIN_OPERATOR_SCAN;
	}
	if (StringUtil::Contains(name, "JOIN") || name == "CROSS_PRODUCT") {
		return HighlightElementType::EXPLAIN_OPERATOR_JOIN;
	}
	if (StringUtil::Contains(name, "AGGREGATE") || StringUtil::Contains(name, "GROUP_BY") ||
	    StringUtil::Contains(name, "DISTINCT") || StringUtil::Contains(name, "WINDOW")) {
		return HighlightElementType::EXPLAIN_OPERATOR_AGGREGATE;
	}
	if (StringUtil::Contains(name, "ORDER_BY") || StringUtil::Contains(name, "TOP_N")) {
		return HighlightElementType::EXPLAIN_OPERATOR_ORDER;
	}
	if (node.children.empty()) {
		// leaf operators that scan a table or call a table function (e.g. RANGE, READ_CSV)
		for (auto &entry : node.details) {
			if (entry.first == "Table" || entry.first == "Function") {
				return HighlightElementType::EXPLAIN_OPERATOR_SCAN;
			}
		}
	}
	return HighlightElementType::EXPLAIN_OPERATOR;
}

//! Remove the redundant outer pair of parentheses wrapping an expression, e.g. "(a >= 1)" -> "a >= 1".
//! Only strips a pair that wraps the entire string (so "contains(a, b)" and "(a) AND (b)" are left untouched).
//! Parentheses inside single-quoted string literals are ignored.
static string StripOuterParens(string text) {
	while (text.size() >= 2 && text.front() == '(' && text.back() == ')') {
		idx_t depth = 0;
		bool in_quote = false;
		bool wraps = true;
		for (idx_t i = 0; i < text.size(); i++) {
			char c = text[i];
			if (c == '\'') {
				in_quote = !in_quote;
				continue;
			}
			if (in_quote) {
				continue;
			}
			if (c == '(') {
				depth++;
			} else if (c == ')') {
				if (depth == 0) {
					wraps = false;
					break;
				}
				depth--;
				// if the first '(' closes before the end, it does not wrap the whole expression
				if (depth == 0 && i + 1 != text.size()) {
					wraps = false;
					break;
				}
			}
		}
		if (!wraps || depth != 0 || in_quote) {
			break;
		}
		text = text.substr(1, text.size() - 2);
	}
	return text;
}

//===--------------------------------------------------------------------===//
// JSON parsing
//===--------------------------------------------------------------------===//
static void ParseExtraInfo(yyjson_val *extra_info, ExplainTreeNode &node) {
	if (!extra_info || !yyjson_is_obj(extra_info)) {
		return;
	}
	duckdb_yyjson::yyjson_obj_iter iter;
	duckdb_yyjson::yyjson_obj_iter_init(extra_info, &iter);
	yyjson_val *key;
	while ((key = duckdb_yyjson::yyjson_obj_iter_next(&iter))) {
		auto val = duckdb_yyjson::yyjson_obj_iter_get_val(key);
		string key_str = yyjson_get_str(key);
		vector<string> values;
		if (yyjson_is_str(val)) {
			values.push_back(yyjson_get_str(val));
		} else if (yyjson_is_arr(val)) {
			size_t idx, max;
			yyjson_val *item;
			yyjson_arr_foreach(val, idx, max, item) {
				if (yyjson_is_str(item)) {
					values.push_back(yyjson_get_str(item));
				}
			}
		}
		if (values.empty() || (values.size() == 1 && values[0].empty())) {
			continue;
		}
		if (key_str == "Estimated Cardinality") {
			node.estimated_rows = idx_t(std::strtoull(values[0].c_str(), nullptr, 10));
			continue;
		}
		// expression values (filters, projections, ...) are often wrapped in a redundant outer pair of
		// parentheses - drop it for readability
		for (auto &value : values) {
			value = StripOuterParens(std::move(value));
		}
		node.details.emplace_back(std::move(key_str), std::move(values));
	}
}

//! Parse a node of an EXPLAIN plan (as rendered by the JSONTreeRenderer)
static unique_ptr<ExplainTreeNode> ParsePlanNode(yyjson_val *obj) {
	if (!obj || !yyjson_is_obj(obj)) {
		return nullptr;
	}
	auto node = make_uniq<ExplainTreeNode>();
	auto name = yyjson_obj_get(obj, "name");
	if (!yyjson_is_str(name)) {
		return nullptr;
	}
	node->name = yyjson_get_str(name);
	ParseExtraInfo(yyjson_obj_get(obj, "extra_info"), *node);
	auto children = yyjson_obj_get(obj, "children");
	if (yyjson_is_arr(children)) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			auto child_node = ParsePlanNode(child);
			if (!child_node) {
				return nullptr;
			}
			node->children.push_back(std::move(child_node));
		}
	}
	return node;
}

//! Parse an operator node of an EXPLAIN ANALYZE profiler output (either the current or the legacy metric format)
static unique_ptr<ExplainTreeNode> ParseProfilerNode(yyjson_val *obj) {
	if (!obj || !yyjson_is_obj(obj)) {
		return nullptr;
	}
	auto node = make_uniq<ExplainTreeNode>();
	auto name = yyjson_obj_get(obj, "type");
	if (!yyjson_is_str(name)) {
		name = yyjson_obj_get(obj, "operator_type");
	}
	if (!yyjson_is_str(name)) {
		return nullptr;
	}
	node->name = yyjson_get_str(name);
	auto timing = yyjson_obj_get(obj, "timing");
	if (!yyjson_is_num(timing)) {
		timing = yyjson_obj_get(obj, "operator_timing");
	}
	if (yyjson_is_num(timing)) {
		node->timing = yyjson_get_num(timing);
	}
	auto rows = yyjson_obj_get(obj, "intermediate_rows");
	if (!yyjson_is_num(rows)) {
		rows = yyjson_obj_get(obj, "operator_cardinality");
	}
	if (yyjson_is_num(rows)) {
		node->rows = idx_t(yyjson_get_num(rows));
	}
	ParseExtraInfo(yyjson_obj_get(obj, "extra_info"), *node);
	auto children = yyjson_obj_get(obj, "children");
	if (yyjson_is_arr(children)) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			auto child_node = ParseProfilerNode(child);
			if (!child_node) {
				return nullptr;
			}
			node->children.push_back(std::move(child_node));
		}
	}
	return node;
}

static double SumOperatorTimings(const ExplainTreeNode &node) {
	double total = node.timing > 0 ? node.timing : 0;
	for (auto &child : node.children) {
		total += SumOperatorTimings(*child);
	}
	return total;
}

//===--------------------------------------------------------------------===//
// Formatting helpers
//===--------------------------------------------------------------------===//
static string FormatCount(idx_t count, char thousand_separator) {
	string result = std::to_string(count);
	if (!thousand_separator) {
		return result;
	}
	string formatted;
	idx_t digits_until_separator = result.size() % 3 == 0 ? 3 : result.size() % 3;
	for (idx_t i = 0; i < result.size(); i++) {
		if (digits_until_separator == 0) {
			formatted += thousand_separator;
			digits_until_separator = 3;
		}
		formatted += result[i];
		digits_until_separator--;
	}
	return formatted;
}

static string FormatTiming(double seconds) {
	if (seconds >= 1.0) {
		return StringUtil::Format("%.2fs", seconds);
	}
	if (seconds >= 0.001) {
		return StringUtil::Format("%.1fms", seconds * 1000.0);
	}
	return StringUtil::Format("%.0fµs", seconds * 1000000.0);
}

static ExplainSpan TimingSpan(double seconds, double total_time) {
	double fraction = total_time > 0 ? seconds / total_time : 0;
	auto text = FormatTiming(seconds);
	// heat-color the timing based on the share of the total query time spent in this operator
	if (fraction >= 0.25) {
		return ExplainSpan(std::move(text), PrintColor::RED, PrintIntensity::BOLD);
	}
	if (fraction >= 0.10) {
		return ExplainSpan(std::move(text), PrintColor::YELLOW, PrintIntensity::STANDARD);
	}
	if (fraction >= 0.01) {
		return ExplainSpan(std::move(text), PrintColor::STANDARD, PrintIntensity::STANDARD);
	}
	return ExplainSpan(std::move(text), PrintColor::GRAY, PrintIntensity::STANDARD);
}

//! Truncate a string to the given render width, appending "…" if it was cut off
static string TruncateText(const string &text, idx_t max_width) {
	if (ShellState::RenderLength(text) <= max_width) {
		return text;
	}
	if (max_width <= 1) {
		return "…";
	}
	string result;
	idx_t width = 0;
	for (idx_t i = 0; i < text.size();) {
		// find the next codepoint
		idx_t next = i + 1;
		while (next < text.size() && !ShellState::IsCharacter(text[next])) {
			next++;
		}
		auto char_width = ShellState::RenderLength(text.c_str() + i, next - i);
		if (width + char_width > max_width - 1) {
			break;
		}
		result += text.substr(i, next - i);
		width += char_width;
		i = next;
	}
	return result + "…";
}

//! Cut a string to the given render width without appending any marker.
static string CutToWidth(const string &text, idx_t width) {
	if (ShellState::RenderLength(text) <= width) {
		return text;
	}
	string result;
	idx_t rendered = 0;
	for (idx_t i = 0; i < text.size();) {
		idx_t next = i + 1;
		while (next < text.size() && !ShellState::IsCharacter(text[next])) {
			next++;
		}
		auto char_width = ShellState::RenderLength(text.c_str() + i, next - i);
		if (rendered + char_width > width) {
			break;
		}
		result += text.substr(i, next - i);
		rendered += char_width;
		i = next;
	}
	return result;
}

//! Word-wrap a single string across lines of the given budget, breaking at spaces (no line limit).
static vector<string> WordWrap(const string &text, idx_t budget) {
	if (budget < 2) {
		budget = 2;
	}
	vector<string> lines;
	string line;
	idx_t line_width = 0;
	for (idx_t i = 0; i < text.size();) {
		idx_t j = i;
		while (j < text.size() && text[j] != ' ') {
			j++;
		}
		string word = text.substr(i, j - i);
		i = j < text.size() ? j + 1 : j;
		if (word.empty()) {
			continue;
		}
		// a single word that is wider than the budget is hard-truncated (rare - most tokens are short)
		if (ShellState::RenderLength(word) > budget) {
			word = TruncateText(word, budget);
		}
		idx_t word_width = ShellState::RenderLength(word);
		idx_t needed = word_width + (line.empty() ? 0 : 1);
		if (line_width + needed <= budget) {
			if (!line.empty()) {
				line += ' ';
				line_width++;
			}
			line += word;
			line_width += word_width;
		} else {
			if (!line.empty()) {
				lines.push_back(std::move(line));
			}
			line = std::move(word);
			line_width = word_width;
		}
	}
	if (!line.empty() || lines.empty()) {
		lines.push_back(std::move(line));
	}
	return lines;
}

//! Wrap a list of values across up to max_lines lines of the given budget. Breaks preferentially between list
//! elements (keeping each element whole), and only word-wraps an element internally when it does not fit on a
//! line by itself. If the values do not fit in max_lines, the last line ends with an "…" marker.
static vector<string> WrapValues(const vector<string> &values, idx_t budget, idx_t max_lines) {
	if (budget < 2) {
		budget = 2;
	}
	vector<string> lines;
	string cur;
	idx_t cur_width = 0;
	for (idx_t k = 0; k < values.size(); k++) {
		// elements are separated by ", " - carry the separating comma at the end of each element
		string piece = k + 1 < values.size() ? values[k] + "," : values[k];
		idx_t piece_width = ShellState::RenderLength(piece);
		idx_t sep = cur.empty() ? 0 : 1;
		if (cur_width + sep + piece_width <= budget) {
			cur = cur.empty() ? std::move(piece) : cur + " " + piece;
			cur_width += sep + piece_width;
			continue;
		}
		// the element does not fit on the current line - flush it and start fresh
		if (!cur.empty()) {
			lines.push_back(std::move(cur));
			cur.clear();
			cur_width = 0;
		}
		if (piece_width <= budget) {
			cur = std::move(piece);
			cur_width = piece_width;
		} else {
			// the element is longer than a full line - word-wrap it internally
			auto sub = WordWrap(piece, budget);
			for (idx_t s = 0; s + 1 < sub.size(); s++) {
				lines.push_back(std::move(sub[s]));
			}
			cur = std::move(sub.back());
			cur_width = ShellState::RenderLength(cur);
		}
	}
	if (!cur.empty() || lines.empty()) {
		lines.push_back(std::move(cur));
	}
	// enforce the line limit, marking the cut-off with an ellipsis
	if (lines.size() > max_lines) {
		lines.resize(max_lines);
		auto &last = lines.back();
		if (ShellState::RenderLength(last) + 2 > budget) {
			last = CutToWidth(last, budget >= 2 ? budget - 2 : 0);
		}
		last += last.empty() ? "…" : " …";
	}
	return lines;
}

//===--------------------------------------------------------------------===//
// Box layout
//===--------------------------------------------------------------------===//
class ExplainBoxRenderer {
public:
	//! Horizontal gap between side-by-side child blocks
	static constexpr idx_t HORIZONTAL_GAP = 2;
	//! Maximum content width within a box. Long details are wrapped (and eventually truncated) instead - full
	//! details remain available through EXPLAIN (FORMAT TEXT / JSON). Kept fairly small so that the (uniform) box
	//! width of a tree stays narrow, letting wide plans like a 6-way join still fit without being split up.
	static constexpr idx_t MAX_BOX_CONTENT_WIDTH = 48;
	//! A detail whose compact "Key: value" form is wider than this is instead rendered with the key on its own line
	//! and the values wrapped underneath. This keeps boxes narrow (bounded by the widest value, not key + value).
	static constexpr idx_t MAX_COMPACT_DETAIL = 40;
	//! Minimum content width for a box that uses the narrow detail layout (so short list values still pack together)
	static constexpr idx_t NARROW_MIN_WIDTH = 30;
	//! Maximum number of lines a single list-valued detail (e.g. a projection list) may wrap across
	static constexpr idx_t MAX_DETAIL_LINES = 5;
	//! Minimum render width we assume to be available
	static constexpr idx_t MINIMUM_RENDER_WIDTH = 30;
	//! Minimum gap between the row count (left) and the timing (right) on the metrics line
	static constexpr idx_t METRICS_GAP = 3;
	//! A tree is only broken into separate "[Subtree #N]" sections once it would render wider than this. Trees that
	//! merely exceed the terminal width are left intact (the pager scrolls horizontally) - only extremely wide trees
	//! (think a 10-way join) are split. A tree narrower than the terminal is centered as usual.
	static constexpr idx_t MAX_TREE_WIDTH = 400;
	//! When flattening, a sub-tree whose cumulative time is below this fraction of the total query time is collapsed
	//! into a single "⋯" node (showing its time). Only sub-trees of at least MIN_FLATTEN_NODES operators are hidden.
	static constexpr double FLATTEN_TIME_FRACTION = 0.01;
	static constexpr idx_t MIN_FLATTEN_NODES = 2;
	//! A run of at least this many consecutive condensed operators in a chain is drawn as one compact grouped box
	static constexpr idx_t MIN_GROUP_NODES = 2;
	//! Maximum number of operator rows shown in a grouped box; the rest are summarized in a trailing "… N more" row
	static constexpr idx_t MAX_GROUP_ROWS = 8;
	//! When flattening, an operator taking less than this fraction of the total query time is not significant enough
	//! to render in full - only its name and metrics are shown (its detail lines are dropped).
	static constexpr double SIGNIFICANT_TIME_FRACTION = 0.05;

	ExplainBoxRenderer(idx_t max_width, char thousand_separator, double total_time, ExplainAlignment alignment,
	                   bool is_console, bool flatten, bool merge)
	    : max_width(MaxValue<idx_t>(max_width, MINIMUM_RENDER_WIDTH)), thousand_separator(thousand_separator),
	      total_time(total_time), alignment(alignment),
	      // only center mode centers the tree in the terminal (and only when writing to a console)
	      center(alignment == ExplainAlignment::CENTER && is_console),
	      layout_width(MaxValue<idx_t>(MaxValue<idx_t>(max_width, MINIMUM_RENDER_WIDTH), MAX_TREE_WIDTH)),
	      flatten(flatten), merge(merge), flatten_threshold(total_time * FLATTEN_TIME_FRACTION),
	      significant_threshold(total_time * SIGNIFICANT_TIME_FRACTION) {
	}

	//! Render the plan as a set of trees. The main tree is rendered first; any sub-tree that is too wide to fit is
	//! replaced inline with a "[Subtree #N]" reference box and rendered separately afterwards under its own header.
	//! This keeps every individual tree balanced and within the terminal width instead of resorting to messy rails.
	//! `main_title` / `main_suffix` make up the header shown above the main tree (e.g. "Query Profile" + timing).
	vector<ExplainLine> Render(const ExplainTreeNode &root, const string &main_title, const string &main_suffix) {
		next_subtree = 0;
		pending.clear();
		pending.push_back({0, &root});

		vector<ExplainLine> result;
		for (idx_t i = 0; i < pending.size(); i++) {
			auto entry = pending[i]; // copy - RenderNode may grow `pending` and invalidate references
			// every node in this tree shares one width (the widest box it contains)
			tree_content_width = MaxValue<idx_t>(TreeContentWidth(*entry.second), 6);
			auto block = RenderNode(*entry.second, layout_width, ChainWidth(*entry.second));
			// in center mode the root of every tree is placed on the exact horizontal centre of the terminal so the
			// main tree and all sub-trees line up down the middle; in left mode everything hugs the left edge
			idx_t indent = RootCenterIndent(block);
			idx_t root_center = indent + block.spine;
			// build the header for this tree (the plan title for the main tree, otherwise the sub-tree reference)
			ExplainLine header;
			if (entry.first == 0) {
				if (!main_title.empty()) {
					header.emplace_back(main_title, HighlightElementType::EXPLAIN_HEADER);
					if (!main_suffix.empty()) {
						header.emplace_back(main_suffix, HighlightElementType::EXPLAIN_DETAIL_KEY);
					}
				}
			} else {
				header.emplace_back(SubtreeLabel(entry.first), HighlightElementType::EXPLAIN_SUBTREE_REF);
			}
			if (!header.empty()) {
				result.emplace_back(); // blank separating line
				if (alignment == ExplainAlignment::CENTER) {
					CenterOver(header, LineWidth(header), root_center);
				}
				result.push_back(std::move(header));
			}
			for (auto &line : block.lines) {
				OffsetLine(line, indent);
				result.push_back(std::move(line));
			}
		}
		return result;
	}

private:
	static string SubtreeLabel(idx_t number) {
		return "[Subtree #" + std::to_string(number) + "]";
	}

	//! Left-pad a line so that its centre lands on render column `center_col`
	static void CenterOver(ExplainLine &line, idx_t line_width, idx_t center_col) {
		idx_t indent = center_col > line_width / 2 ? center_col - line_width / 2 : 0;
		OffsetLine(line, indent);
	}

	//! The indentation that puts the tree's root node at the centre of the terminal (clamped so the block stays on
	//! screen). Returns 0 when not rendering to a console (output stays left-aligned).
	idx_t RootCenterIndent(const ExplainBlock &block) const {
		if (!center) {
			return 0;
		}
		idx_t block_width = 0;
		for (auto &line : block.lines) {
			block_width = MaxValue<idx_t>(block_width, LineWidth(line));
		}
		idx_t ideal = max_width / 2 > block.spine ? max_width / 2 - block.spine : 0;
		idx_t max_indent = max_width > block_width ? max_width - block_width : 0;
		return MinValue<idx_t>(ideal, max_indent);
	}

	static idx_t LongestValue(const vector<string> &values) {
		idx_t longest = 0;
		for (auto &value : values) {
			longest = MaxValue<idx_t>(longest, ShellState::RenderLength(value));
		}
		return longest;
	}

	//! Whether a detail is rendered with its key on its own line (true) or compactly as "Key: value" (false)
	static bool DetailIsNarrow(const std::pair<string, vector<string>> &entry) {
		if (entry.first == "Text") {
			return false;
		}
		idx_t compact = ShellState::RenderLength(entry.first) + 2 + LongestValue(entry.second);
		return compact > MAX_COMPACT_DETAIL;
	}

	//! Content width a single detail needs, in whichever layout (compact / narrow) it will use
	static idx_t DetailWidth(const std::pair<string, vector<string>> &entry, idx_t max_content) {
		// a value that is not the last in a list carries a trailing comma, so reserve a column for it
		idx_t longest = LongestValue(entry.second) + (entry.second.size() > 1 ? 1 : 0);
		if (!DetailIsNarrow(entry)) {
			// compact: "Key: value" on one line
			idx_t key = entry.first == "Text" ? 0 : ShellState::RenderLength(entry.first) + 2;
			return key + longest;
		}
		// narrow: the box need only be as wide as the longest value (or the key, whichever is wider)
		idx_t narrow = MaxValue<idx_t>(ShellState::RenderLength(entry.first), MinValue<idx_t>(longest, max_content));
		return MaxValue<idx_t>(narrow, NARROW_MIN_WIDTH);
	}

	//! Whether `node` is rendered condensed (just its name, essential details and metrics) because it takes too small
	//! a share of the query time to be worth showing in full.
	bool IsCondensed(const ExplainTreeNode &node) const {
		return flatten && node.timing >= 0 && node.timing < significant_threshold;
	}

	//! Details that are always shown, even when a box is condensed - currently the table name of a scan, so a scan is
	//! never reduced to an anonymous box.
	static bool IsEssentialDetail(const std::pair<string, vector<string>> &entry) {
		return entry.first == "Table";
	}

	//! The content width an individual box needs to render its own content (using the narrow layout for wide details)
	idx_t BoxContentWidth(const ExplainTreeNode &node) {
		idx_t max_content = MinValue<idx_t>(layout_width - 4, MAX_BOX_CONTENT_WIDTH);
		bool condensed = IsCondensed(node);
		idx_t width = ShellState::RenderLength(node.name);
		// a condensed box only shows its name, essential details and metrics
		for (auto &entry : node.details) {
			if (condensed && !IsEssentialDetail(entry)) {
				continue;
			}
			width = MaxValue<idx_t>(width, DetailWidth(entry, max_content));
		}
		width = MaxValue<idx_t>(width, MetricsWidth(node));
		return MaxValue<idx_t>(MinValue<idx_t>(width, max_content), 6);
	}

	//! The widest content any box in the (sub)tree needs - used by center mode, where every box in a tree shares one
	//! width so that nodes line up.
	idx_t TreeContentWidth(const ExplainTreeNode &node) {
		idx_t width = BoxContentWidth(node);
		for (auto &child : node.children) {
			width = MaxValue<idx_t>(width, TreeContentWidth(*child));
		}
		return width;
	}

	//! The shared content width of a "trunk": a node and the chain of first-children below it (the main data-flow
	//! pipeline, drawn down the left edge in left mode). These boxes are dependent on one another and are rendered at
	//! this common width so they line up; side branches (a fork's other children) form their own trunks.
	idx_t ChainWidth(const ExplainTreeNode &node) {
		idx_t width = 6; // minimum
		const ExplainTreeNode *current = &node;
		while (true) {
			if (IsCollapsible(*current)) {
				// a collapsed sub-tree is drawn as a grouped box (a row per operator) and ends the trunk
				width = MaxValue<idx_t>(width, SubtreeGroupedWidth(*current));
				break;
			}
			// a grouped operator is drawn as a single row (name + metrics on one line), which may be wider than the
			// box it would otherwise need; size the trunk for that so the grouped box still fits
			if (IsGroupable(*current)) {
				width = MaxValue<idx_t>(width, GroupedRowWidth(*current));
			} else {
				width = MaxValue<idx_t>(width, BoxContentWidth(*current));
			}
			if (current->children.empty()) {
				break;
			}
			// follow the trunk down through the first child (across forks)
			current = current->children[0].get();
		}
		return width;
	}

	//! Content width used for a node's box. In left mode every box in a chain shares one width (so a pipeline lines
	//! up) while independent sub-trees are sized to their own content; in center mode the whole tree shares a width.
	idx_t EffectiveWidth(idx_t chain_width) const {
		return alignment == ExplainAlignment::LEFT ? chain_width : tree_content_width;
	}

	//! Whether the sub-tree rooted at `node` is collapsed into a single "⋯" node because it contributes little to the
	//! overall query time. Merging several operators into one is only done for large plans (see `merge`).
	bool IsCollapsible(const ExplainTreeNode &node) const {
		return merge && node.subtree_count >= MIN_FLATTEN_NODES && node.subtree_time < flatten_threshold;
	}

	//! Whether `node` is a condensed operator inside a chain - several of these in a row are drawn together in one
	//! compact "grouped" box (one row each) instead of a full box per operator. Fork/leaf and collapsed nodes are not.
	bool IsGroupable(const ExplainTreeNode &node) const {
		return IsCondensed(node) && node.children.size() == 1 && !IsCollapsible(node);
	}

	//! The number of condensed operators grouped together starting at `node` (consecutive groupable nodes)
	idx_t GroupRunLength(const ExplainTreeNode &node) const {
		idx_t count = 0;
		const ExplainTreeNode *current = &node;
		while (IsGroupable(*current)) {
			count++;
			current = current->children[0].get();
		}
		return count;
	}

	//! The node directly below a grouped run of `count` operators starting at `node`
	const ExplainTreeNode &GroupRunAfter(const ExplainTreeNode &node, idx_t count) const {
		const ExplainTreeNode *current = &node;
		for (idx_t i = 0; i < count; i++) {
			current = current->children[0].get();
		}
		return *current;
	}

	//! The "N rows · Tµs" metrics shown on a single grouped-operator row
	ExplainLine GroupedMetrics(const ExplainTreeNode &node) {
		ExplainLine metrics;
		if (node.rows.IsValid()) {
			auto rows = node.rows.GetIndex();
			metrics.emplace_back(FormatCount(rows, thousand_separator) + (rows == 1 ? " row" : " rows"),
			                     HighlightElementType::EXPLAIN_ROWS);
		} else if (node.estimated_rows.IsValid()) {
			auto estimate = node.estimated_rows.GetIndex();
			metrics.emplace_back("~" + FormatCount(estimate, thousand_separator) + (estimate == 1 ? " row" : " rows"),
			                     HighlightElementType::EXPLAIN_DETAIL_KEY);
		}
		if (node.timing >= 0) {
			if (!metrics.empty()) {
				metrics.emplace_back(" · ", HighlightElementType::EXPLAIN_DETAIL_KEY);
			}
			metrics.push_back(TimingSpan(node.timing, total_time));
		}
		return metrics;
	}

	//! The label shown on a grouped-operator row - the operator name, with the table appended for a scan
	string GroupedRowName(const ExplainTreeNode &node) {
		string name = DisplayName(node.name);
		for (auto &entry : node.details) {
			if (entry.first == "Table" && !entry.second.empty()) {
				const string &table = entry.second.front();
				auto pos = table.find_last_of('.');
				return name + " " + (pos == string::npos ? table : table.substr(pos + 1));
			}
		}
		return name;
	}

	//! Width of one grouped-operator row: the operator label on the left, the metrics on the right
	idx_t GroupedRowWidth(const ExplainTreeNode &node) {
		return ShellState::RenderLength(GroupedRowName(node)) + METRICS_GAP + LineWidth(GroupedMetrics(node));
	}

	//! The widest grouped row needed by the whole sub-tree rooted at `node` (used to size a collapsed sub-tree box)
	idx_t SubtreeGroupedWidth(const ExplainTreeNode &node) {
		idx_t width = GroupedRowWidth(node);
		for (auto &child : node.children) {
			width = MaxValue<idx_t>(width, SubtreeGroupedWidth(*child));
		}
		return width;
	}

	//! Content width of a "[Subtree #N]" reference box (small in left mode, the shared tree width in center mode)
	idx_t PlaceholderContentWidth() const {
		return alignment == ExplainAlignment::LEFT ? ShellState::RenderLength(string("[Subtree #00]"))
		                                           : tree_content_width;
	}

	//! The metrics of a box, split into a left part (row count) and a right part (timing) so that the timing can
	//! be right-aligned against the box border. For EXPLAIN (no ANALYZE) only the estimated row count is shown.
	struct BoxMetrics {
		ExplainLine left;
		ExplainLine right;

		bool Empty() const {
			return left.empty() && right.empty();
		}
	};

	BoxMetrics BuildMetrics(const ExplainTreeNode &node) {
		BoxMetrics metrics;
		if (node.rows.IsValid()) {
			auto rows = node.rows.GetIndex();
			metrics.left.emplace_back(FormatCount(rows, thousand_separator) + (rows == 1 ? " row" : " rows"),
			                          HighlightElementType::EXPLAIN_ROWS);
		} else if (node.estimated_rows.IsValid()) {
			auto estimate = node.estimated_rows.GetIndex();
			metrics.left.emplace_back("~" + FormatCount(estimate, thousand_separator) +
			                              (estimate == 1 ? " row" : " rows"),
			                          HighlightElementType::EXPLAIN_DETAIL_KEY);
		}
		if (node.timing >= 0) {
			metrics.right.push_back(TimingSpan(node.timing, total_time));
		}
		return metrics;
	}

	//! Render width required by the metrics line (row count on the left, timing on the right)
	idx_t MetricsWidth(const ExplainTreeNode &node) {
		auto metrics = BuildMetrics(node);
		idx_t left_width = LineWidth(metrics.left);
		idx_t right_width = LineWidth(metrics.right);
		if (left_width > 0 && right_width > 0) {
			return left_width + METRICS_GAP + right_width;
		}
		return MaxValue<idx_t>(left_width, right_width);
	}

	//! Render a single operator box (without children)
	ExplainBlock BuildBox(const ExplainTreeNode &node, idx_t chain_width) {
		idx_t content_width = EffectiveWidth(chain_width);

		// gather the content lines of the box - a condensed box drops its details and shows only name + metrics
		vector<ExplainLine> content;
		bool condensed = IsCondensed(node);
		for (auto &entry : node.details) {
			if (condensed && !IsEssentialDetail(entry)) {
				continue;
			}
			bool has_key = entry.first != "Text";
			if (has_key && DetailIsNarrow(entry)) {
				// narrow layout: render the key on its own line, then the values wrapped underneath at full width
				content.emplace_back();
				content.back().emplace_back(TruncateText(entry.first, content_width),
				                            HighlightElementType::EXPLAIN_DETAIL_KEY);
				for (auto &value_line : WrapValues(entry.second, content_width, MAX_DETAIL_LINES)) {
					content.emplace_back();
					content.back().emplace_back(std::move(value_line), HighlightElementType::EXPLAIN_DETAIL_VALUE);
				}
				continue;
			}
			// compact layout: "Key: value", with continuation lines indented under the first value
			string key = has_key ? entry.first + ": " : string();
			idx_t key_width = ShellState::RenderLength(key);
			if (has_key && key_width >= content_width) {
				key = TruncateText(key, content_width >= 1 ? content_width - 1 : 1);
				key_width = ShellState::RenderLength(key);
			}
			auto value_lines = WrapValues(entry.second, content_width - key_width, MAX_DETAIL_LINES);
			for (idx_t li = 0; li < value_lines.size(); li++) {
				ExplainLine line;
				if (li == 0 && has_key) {
					line.emplace_back(key, HighlightElementType::EXPLAIN_DETAIL_KEY);
				} else if (has_key) {
					line.emplace_back(string(key_width, ' '), HighlightElementType::NONE);
				}
				line.emplace_back(std::move(value_lines[li]), HighlightElementType::EXPLAIN_DETAIL_VALUE);
				content.push_back(std::move(line));
			}
		}
		// metrics line: row count on the left, timing right-aligned against the box border
		auto metrics = BuildMetrics(node);
		if (!metrics.Empty()) {
			idx_t left_width = LineWidth(metrics.left);
			idx_t right_width = LineWidth(metrics.right);
			ExplainLine line;
			for (auto &span : metrics.left) {
				line.push_back(std::move(span));
			}
			if (right_width > 0) {
				// pad between the row count and the timing so the timing sits flush against the right border
				idx_t gap = content_width > left_width + right_width ? content_width - left_width - right_width : 1;
				line.emplace_back(string(gap, ' '), HighlightElementType::NONE);
				for (auto &span : metrics.right) {
					line.push_back(std::move(span));
				}
			}
			content.push_back(std::move(line));
		}

		// the title is rendered into the top border - truncate it so at least one dash remains on either side
		string title = TruncateText(DisplayName(node.name), content_width >= 2 ? content_width - 2 : 1);
		idx_t title_width = ShellState::RenderLength(title);
		idx_t box_width = content_width + 4;

		ExplainBlock block;
		// top border with the operator name: ╭─ NAME ────╮
		ExplainLine top;
		top.emplace_back("╭─ ", HighlightElementType::EXPLAIN_LAYOUT);
		top.emplace_back(std::move(title), GetOperatorElement(node));
		top.emplace_back(" " + StringUtil::Repeat("─", box_width - 5 - title_width) + "╮",
		                 HighlightElementType::EXPLAIN_LAYOUT);
		block.lines.push_back(std::move(top));
		// content lines: │ content │
		for (auto &line : content) {
			ExplainLine content_line;
			content_line.emplace_back("│ ", HighlightElementType::EXPLAIN_LAYOUT);
			idx_t line_width = LineWidth(line);
			for (auto &span : line) {
				content_line.push_back(std::move(span));
			}
			idx_t padding = content_width >= line_width ? content_width - line_width : 0;
			content_line.emplace_back(string(padding + 1, ' ') + "│", HighlightElementType::EXPLAIN_LAYOUT);
			block.lines.push_back(std::move(content_line));
		}
		// bottom border: ╰────╯
		ExplainLine bottom;
		bottom.emplace_back("╰" + StringUtil::Repeat("─", box_width - 2) + "╯", HighlightElementType::EXPLAIN_LAYOUT);
		block.lines.push_back(std::move(bottom));
		block.width = box_width;
		block.spine = box_width / 2;
		return block;
	}

	//! Append one grouped-box row: a left label (colored) and a metrics line right-aligned against the border
	void AddGroupedRow(ExplainBlock &block, const string &label, HighlightElementType label_color, ExplainLine metrics,
	                   idx_t content_width) {
		string text = TruncateText(label, content_width);
		idx_t name_width = ShellState::RenderLength(text);
		idx_t metrics_width = LineWidth(metrics);
		ExplainLine line;
		line.emplace_back("│ ", HighlightElementType::EXPLAIN_LAYOUT);
		line.emplace_back(std::move(text), label_color);
		idx_t used = name_width + metrics_width;
		idx_t gap = content_width > used ? content_width - used : 1;
		line.emplace_back(string(gap, ' '), HighlightElementType::NONE);
		for (auto &span : metrics) {
			line.push_back(std::move(span));
		}
		idx_t line_content = name_width + gap + metrics_width;
		idx_t padding = content_width >= line_content ? content_width - line_content : 0;
		line.emplace_back(string(padding + 1, ' ') + "│", HighlightElementType::EXPLAIN_LAYOUT);
		block.lines.push_back(std::move(line));
	}

	//! A compact box grouping several low-significance operators, one row per operator (name + rows/time). Used both
	//! for a run of condensed operators in a pipeline and for a collapsed sub-tree. Long lists are capped with a
	//! trailing "… N more" row so the box stays bounded.
	ExplainBlock BuildGroupedBox(const vector<const ExplainTreeNode *> &nodes, idx_t chain_width) {
		idx_t content_width = EffectiveWidth(chain_width);
		idx_t box_width = content_width + 4;

		ExplainBlock block;
		// plain top border (the operator names live in the rows, not the border)
		block.lines.emplace_back();
		block.lines.back().emplace_back("╭" + StringUtil::Repeat("─", box_width - 2) + "╮",
		                                HighlightElementType::EXPLAIN_LAYOUT);

		idx_t shown = MinValue<idx_t>(nodes.size(), MAX_GROUP_ROWS);
		for (idx_t i = 0; i < shown; i++) {
			AddGroupedRow(block, GroupedRowName(*nodes[i]), GetOperatorElement(*nodes[i]), GroupedMetrics(*nodes[i]),
			              content_width);
		}
		if (nodes.size() > shown) {
			// summarize the remainder in one trailing row
			idx_t more = nodes.size() - shown;
			double remaining = 0;
			for (idx_t i = shown; i < nodes.size(); i++) {
				remaining += nodes[i]->timing > 0 ? nodes[i]->timing : 0;
			}
			ExplainLine metrics;
			if (remaining > 0) {
				metrics.push_back(TimingSpan(remaining, total_time));
			}
			AddGroupedRow(block, "… " + std::to_string(more) + " more", HighlightElementType::EXPLAIN_DETAIL_KEY,
			              std::move(metrics), content_width);
		}

		block.lines.emplace_back();
		block.lines.back().emplace_back("╰" + StringUtil::Repeat("─", box_width - 2) + "╯",
		                                HighlightElementType::EXPLAIN_LAYOUT);
		block.width = box_width;
		block.spine = box_width / 2;
		return block;
	}

	//! The operators of a run of `count` condensed nodes starting at `node` (one row each in a grouped box)
	vector<const ExplainTreeNode *> GroupRunNodes(const ExplainTreeNode &node, idx_t count) {
		vector<const ExplainTreeNode *> nodes;
		const ExplainTreeNode *current = &node;
		for (idx_t i = 0; i < count; i++) {
			nodes.push_back(current);
			current = current->children[0].get();
		}
		return nodes;
	}

	//! All operators in the sub-tree rooted at `node`, in depth-first order (for a collapsed grouped box)
	void CollectSubtreeNodes(const ExplainTreeNode &node, vector<const ExplainTreeNode *> &nodes) {
		nodes.push_back(&node);
		for (auto &child : node.children) {
			CollectSubtreeNodes(*child, nodes);
		}
	}

	//! Stack a box on top of a single child block, aligning the connection points
	ExplainBlock StackChain(ExplainBlock box, ExplainBlock child) {
		idx_t target = MaxValue<idx_t>(box.spine, child.spine);
		idx_t box_offset = target - box.spine;
		idx_t child_offset = target - child.spine;
		// draw the connectors: ┬ on the bottom border of the box, ┴ on the top border of the child
		SetLayoutChar(box.lines.back(), box.spine, "┬");
		DrawRoofConnector(child, child.spine);

		ExplainBlock result;
		for (auto &line : box.lines) {
			OffsetLine(line, box_offset);
			result.lines.push_back(std::move(line));
		}
		for (auto &line : child.lines) {
			OffsetLine(line, child_offset);
			result.lines.push_back(std::move(line));
		}
		result.width = MaxValue<idx_t>(box_offset + box.width, child_offset + child.width);
		result.spine = target;
		return result;
	}

	//! Place children side-by-side below the box, dispatching to the layout for the current alignment
	ExplainBlock AttachHorizontal(ExplainBlock box, vector<ExplainBlock> &children) {
		if (alignment == ExplainAlignment::LEFT) {
			return AttachHorizontalLeft(std::move(box), children);
		}
		return AttachHorizontalCentered(std::move(box), children);
	}

	//! Lay the children out in a row, returning the row blocks plus the per-child spine columns. In left mode the
	//! children are packed tightly using their contours (so a short sibling can tuck in next to a deep one rather
	//! than being pushed past its full width); in center mode they are simply placed side-by-side.
	void LayoutRow(vector<ExplainBlock> &children, vector<ExplainLine> &row_lines, vector<idx_t> &spines,
	               idx_t &row_width) {
		bool compact = alignment == ExplainAlignment::LEFT;
		vector<idx_t> offsets(children.size(), 0);
		vector<idx_t> profile; // per row: rightmost occupied column + 1 (0 = empty)
		idx_t cumulative = 0;
		for (idx_t i = 0; i < children.size(); i++) {
			DrawRoofConnector(children[i], children[i].spine);
			auto &lines = children[i].lines;
			idx_t offset = cumulative;
			if (compact) {
				offset = 0;
				for (idx_t r = 0; r < lines.size() && r < profile.size(); r++) {
					if (profile[r] == 0) {
						continue;
					}
					idx_t need = profile[r] + HORIZONTAL_GAP; // this child must start (content) at >= here
					idx_t left = LeadingSpaces(lines[r]);
					if (need > left) {
						offset = MaxValue<idx_t>(offset, need - left);
					}
				}
			}
			offsets[i] = offset;
			spines.push_back(offset + children[i].spine);
			for (idx_t r = 0; r < lines.size(); r++) {
				if (r >= profile.size()) {
					profile.resize(r + 1, 0);
				}
				profile[r] = MaxValue<idx_t>(profile[r], offset + LineWidth(lines[r]));
			}
			cumulative = offset + children[i].width + HORIZONTAL_GAP;
		}
		row_width = 0;
		for (auto width : profile) {
			row_width = MaxValue<idx_t>(row_width, width);
		}
		for (idx_t line_idx = 0; line_idx < profile.size(); line_idx++) {
			ExplainLine line;
			for (idx_t i = 0; i < children.size(); i++) {
				if (line_idx < children[i].lines.size()) {
					auto &child_line = children[i].lines[line_idx];
					AppendAt(line, child_line, offsets[i] + LeadingSpaces(child_line));
				}
			}
			row_lines.push_back(std::move(line));
		}
	}

	//! Left-anchored layout: the parent box stays at column 0 and drops straight down to the first child, with a
	//! distributor fanning out to the remaining children. Keeps the root at the top-left (the old explain plan look):
	//!     ╭─ PARENT ─╮
	//!     ╰─────┬────╯
	//!           ├──────────────╮
	//!     ╭─ A ─┴────╮    ╭─ B ─┴────╮
	ExplainBlock AttachHorizontalLeft(ExplainBlock box, vector<ExplainBlock> &children) {
		vector<ExplainLine> row_lines;
		vector<idx_t> spines;
		idx_t row_width = 0;
		LayoutRow(children, row_lines, spines, row_width);

		SetLayoutChar(box.lines.back(), box.spine, "┬");
		ExplainBlock result;
		for (auto &line : box.lines) {
			result.lines.push_back(std::move(line));
		}
		// distributor line: ├───┬───────╮ from the parent spine out to each child
		string routing;
		for (idx_t col = spines.front(); col <= spines.back(); col++) {
			bool is_child_spine = std::find(spines.begin(), spines.end(), col) != spines.end();
			const char *c = "─";
			if (col == spines.front()) {
				c = "├";
			} else if (col == spines.back()) {
				c = "╮";
			} else if (is_child_spine) {
				c = "┬";
			}
			routing += c;
		}
		ExplainLine routing_line;
		routing_line.emplace_back(std::move(routing), HighlightElementType::EXPLAIN_LAYOUT);
		OffsetLine(routing_line, spines.front());
		result.lines.push_back(std::move(routing_line));
		for (auto &line : row_lines) {
			result.lines.push_back(std::move(line));
		}
		result.width = MaxValue<idx_t>(box.width, row_width);
		result.spine = box.spine;
		return result;
	}

	//! Centered layout: the parent is centered over its children, which fan out to the left and right:
	//!          ╭─ PARENT ─╮
	//!       ╭──────┴──────╮
	//!     ╭─ A ─╮     ╭─ B ─╮
	ExplainBlock AttachHorizontalCentered(ExplainBlock box, vector<ExplainBlock> &children) {
		vector<ExplainLine> row_lines;
		vector<idx_t> spines;
		idx_t row_width = 0;
		LayoutRow(children, row_lines, spines, row_width);

		// center the parent box over the span between the first and last child
		idx_t parent_spine = (spines.front() + spines.back()) / 2;
		idx_t box_offset = 0;
		idx_t row_offset = 0;
		if (parent_spine > box.spine) {
			box_offset = parent_spine - box.spine;
		} else {
			row_offset = box.spine - parent_spine;
			for (auto &spine : spines) {
				spine += row_offset;
			}
			parent_spine = box.spine;
		}

		// can the children connect straight onto the parent's bottom border, or do we need a routing line below it?
		bool direct = true;
		for (auto spine : spines) {
			if (spine <= box_offset || spine >= box_offset + box.width - 1) {
				direct = false;
				break;
			}
		}
		ExplainBlock result;
		if (direct) {
			// children connect directly to the bottom border: ╰───┬─────┬───╯
			for (auto spine : spines) {
				SetLayoutChar(box.lines.back(), spine - box_offset, "┬");
			}
		} else {
			SetLayoutChar(box.lines.back(), box.spine, "┬");
		}
		for (auto &line : box.lines) {
			OffsetLine(line, box_offset);
			result.lines.push_back(std::move(line));
		}
		if (!direct) {
			// routing line: ╭──────┴──────╮ with ┬ for any children in the middle
			string routing;
			for (idx_t col = spines.front(); col <= spines.back(); col++) {
				const char *c = "─";
				bool is_child_spine = std::find(spines.begin(), spines.end(), col) != spines.end();
				if (col == spines.front()) {
					c = is_child_spine && col == parent_spine ? "├" : "╭";
				} else if (col == spines.back()) {
					c = is_child_spine && col == parent_spine ? "┤" : "╮";
				} else if (is_child_spine) {
					c = col == parent_spine ? "┼" : "┬";
				} else if (col == parent_spine) {
					c = "┴";
				}
				routing += c;
			}
			ExplainLine routing_line;
			routing_line.emplace_back(std::move(routing), HighlightElementType::EXPLAIN_LAYOUT);
			OffsetLine(routing_line, spines.front());
			result.lines.push_back(std::move(routing_line));
		}
		for (auto &line : row_lines) {
			OffsetLine(line, row_offset);
			result.lines.push_back(std::move(line));
		}
		result.width = MaxValue<idx_t>(box_offset + box.width, row_offset + row_width);
		result.spine = box_offset + box.spine;
		return result;
	}

	//! Width of a "[Subtree #N]" reference box
	idx_t PlaceholderWidth() const {
		return PlaceholderContentWidth() + 4;
	}

	//! A "[Subtree #N]" reference box that stands in for a sub-tree that is rendered separately.
	ExplainBlock BuildLabelBox(const string &label) {
		idx_t content_width = MaxValue<idx_t>(PlaceholderContentWidth(), ShellState::RenderLength(label) + 2);
		string title = TruncateText(label, content_width >= 2 ? content_width - 2 : 1);
		idx_t title_width = ShellState::RenderLength(title);
		idx_t box_width = content_width + 4;

		ExplainBlock block;
		ExplainLine top;
		top.emplace_back("╭─ ", HighlightElementType::EXPLAIN_LAYOUT);
		top.emplace_back(std::move(title), HighlightElementType::EXPLAIN_SUBTREE_REF);
		top.emplace_back(" " + StringUtil::Repeat("─", box_width - 5 - title_width) + "╮",
		                 HighlightElementType::EXPLAIN_LAYOUT);
		block.lines.push_back(std::move(top));
		ExplainLine bottom;
		bottom.emplace_back("╰" + StringUtil::Repeat("─", box_width - 2) + "╯", HighlightElementType::EXPLAIN_LAYOUT);
		block.lines.push_back(std::move(bottom));
		block.width = box_width;
		block.spine = box_width / 2;
		return block;
	}

	//! The width and spine (root-box connection column) of a rendered block - the geometry needed to predict how
	//! wide a sub-tree will be without actually rendering it.
	struct Extent {
		idx_t width = 0;
		idx_t spine = 0;
	};

	//! Geometry of stacking a box on top of a single child (mirrors StackChain)
	static Extent StackExtent(Extent box, Extent child) {
		idx_t target = MaxValue<idx_t>(box.spine, child.spine);
		idx_t box_offset = target - box.spine;
		idx_t child_offset = target - child.spine;
		return {MaxValue<idx_t>(box_offset + box.width, child_offset + child.width), target};
	}

	//! Geometry of attaching a box over a row of children, mirroring the actual layout for the current alignment.
	Extent ForkExtent(Extent box, const vector<Extent> &children) {
		idx_t row_width = 0;
		vector<idx_t> spines;
		for (idx_t i = 0; i < children.size(); i++) {
			spines.push_back(row_width + children[i].spine);
			row_width += children[i].width + (i + 1 < children.size() ? HORIZONTAL_GAP : 0);
		}
		if (alignment == ExplainAlignment::LEFT) {
			// left-anchored: the box stays at column 0
			return {MaxValue<idx_t>(box.width, row_width), box.spine};
		}
		// centered: the parent box overhangs the children row (the overhang accumulates in deep unbalanced trees)
		idx_t parent_spine = (spines.front() + spines.back()) / 2;
		idx_t box_offset = 0;
		idx_t row_offset = 0;
		if (parent_spine > box.spine) {
			box_offset = parent_spine - box.spine;
		} else {
			row_offset = box.spine - parent_spine;
		}
		return {MaxValue<idx_t>(box_offset + box.width, row_offset + row_width), box_offset + box.spine};
	}

	Extent BoxExtent(idx_t chain_width) {
		idx_t box_width = EffectiveWidth(chain_width) + 4;
		return {box_width, box_width / 2};
	}

	//! Decide which children to defer so the centered fork fits in `avail`. The widest sub-tree children are deferred
	//! first; leaf children cannot be deferred (rendering them separately is pointless).
	vector<bool> DecideDeferred(Extent box, const vector<Extent> &child_extents, const vector<bool> &has_children,
	                            Extent placeholder, idx_t avail) {
		vector<bool> deferred(child_extents.size(), false);
		while (true) {
			vector<Extent> row;
			for (idx_t i = 0; i < child_extents.size(); i++) {
				row.push_back(deferred[i] ? placeholder : child_extents[i]);
			}
			if (ForkExtent(box, row).width <= avail) {
				break;
			}
			idx_t best = child_extents.size();
			for (idx_t i = 0; i < child_extents.size(); i++) {
				if (!deferred[i] && has_children[i] &&
				    (best == child_extents.size() || child_extents[i].width > child_extents[best].width)) {
					best = i;
				}
			}
			if (best == child_extents.size()) {
				break; // nothing left we can defer
			}
			deferred[best] = true;
		}
		return deferred;
	}

	//! Estimate the layout extent of the sub-tree rooted at `node` (mirrors RenderNode so the two agree on widths).
	//! `chain_width` is the shared width of the chain `node` belongs to.
	Extent EstimateLayout(const ExplainTreeNode &node, idx_t avail, idx_t chain_width) {
		avail = MaxValue<idx_t>(avail, MINIMUM_RENDER_WIDTH);
		if (IsCollapsible(node)) {
			// a collapsed sub-tree renders as one grouped box at the chain width
			return BoxExtent(chain_width);
		}
		if (node.children.size() == 1) {
			// a grouped run renders as one box but at the same width, so the geometry is identical either way
			idx_t run = GroupRunLength(node);
			const ExplainTreeNode &next = run >= MIN_GROUP_NODES ? GroupRunAfter(node, run) : *node.children[0];
			return StackExtent(BoxExtent(chain_width), EstimateLayout(next, avail, chain_width));
		}
		Extent box = BoxExtent(chain_width);
		if (node.children.empty()) {
			return box;
		}
		vector<Extent> child_extents;
		vector<bool> has_children;
		for (idx_t i = 0; i < node.children.size(); i++) {
			auto &child = node.children[i];
			// the first child continues the trunk (shares the parent's width); the rest start their own trunks
			idx_t child_chain = i == 0 ? chain_width : ChainWidth(*child);
			child_extents.push_back(EstimateLayout(*child, avail, child_chain));
			has_children.push_back(!child->children.empty());
		}
		Extent placeholder = {PlaceholderWidth(), PlaceholderWidth() / 2};
		auto deferred = DecideDeferred(box, child_extents, has_children, placeholder, avail);
		vector<Extent> row;
		for (idx_t i = 0; i < child_extents.size(); i++) {
			row.push_back(deferred[i] ? placeholder : child_extents[i]);
		}
		return ForkExtent(box, row);
	}

	//! Render the (sub)tree rooted at `node`. A child sub-tree that is too wide to fit alongside its siblings is
	//! replaced by a "[Subtree #N]" reference box and queued to be rendered separately. `chain_width` is the shared
	//! width of the chain `node` belongs to (its run of single-child descendants).
	ExplainBlock RenderNode(const ExplainTreeNode &node, idx_t avail, idx_t chain_width) {
		avail = MaxValue<idx_t>(avail, MINIMUM_RENDER_WIDTH);
		if (IsCollapsible(node)) {
			// low-impact sub-tree - draw it as one compact grouped box, one row per operator
			vector<const ExplainTreeNode *> nodes;
			CollectSubtreeNodes(node, nodes);
			return BuildGroupedBox(nodes, chain_width);
		}
		if (node.children.size() == 1) {
			idx_t run = GroupRunLength(node);
			if (run >= MIN_GROUP_NODES) {
				// draw a run of condensed operators as one compact box (one row each), then continue the chain below
				auto &after = GroupRunAfter(node, run);
				return StackChain(BuildGroupedBox(GroupRunNodes(node, run), chain_width),
				                  RenderNode(after, avail, chain_width));
			}
			// same chain - keep the shared width
			return StackChain(BuildBox(node, chain_width), RenderNode(*node.children[0], avail, chain_width));
		}
		auto box = BuildBox(node, chain_width);
		if (node.children.empty()) {
			return box;
		}
		// decide which children fit and which must be deferred (based on a dry-run layout estimate)
		// the first child continues the trunk (shares the parent's width); the rest start their own trunks
		vector<idx_t> child_chains;
		vector<Extent> child_extents;
		vector<bool> has_children;
		for (idx_t i = 0; i < node.children.size(); i++) {
			child_chains.push_back(i == 0 ? chain_width : ChainWidth(*node.children[i]));
			child_extents.push_back(EstimateLayout(*node.children[i], avail, child_chains[i]));
			has_children.push_back(!node.children[i]->children.empty());
		}
		Extent placeholder = {PlaceholderWidth(), PlaceholderWidth() / 2};
		auto deferred = DecideDeferred(BoxExtent(chain_width), child_extents, has_children, placeholder, avail);
		vector<ExplainBlock> children;
		for (idx_t i = 0; i < node.children.size(); i++) {
			if (deferred[i]) {
				idx_t number = ++next_subtree;
				pending.push_back({number, node.children[i].get()});
				children.push_back(BuildLabelBox(SubtreeLabel(number)));
			} else {
				children.push_back(RenderNode(*node.children[i], avail, child_chains[i]));
			}
		}
		return AttachHorizontal(std::move(box), children);
	}

private:
	idx_t max_width;
	char thousand_separator;
	double total_time;
	//! Left or center tree layout
	ExplainAlignment alignment;
	//! Whether to centre each tree in the terminal
	bool center;
	//! The width a tree may occupy before it is broken into sub-trees (>= the terminal width; the pager scrolls)
	idx_t layout_width;
	//! Whether significance-based rendering is active (i.e. we have timing) - drives condensing of individual operators
	bool flatten;
	//! Whether several low-impact operators may be merged into a single "⋯" node (folding a run / collapsing a
	//! sub-tree). Only enabled for large plans - small plans keep every operator as its own box.
	bool merge;
	//! Cumulative time below which a sub-tree is collapsed when flattening
	double flatten_threshold;
	//! Own time below which an operator is rendered condensed (name + metrics only, no detail lines)
	double significant_threshold;
	//! The shared box width used for every box in the tree currently being rendered (set per tree)
	idx_t tree_content_width = 0;
	//! Running counter for "[Subtree #N]" references
	idx_t next_subtree = 0;
	//! Work-list of trees still to render: (subtree number or 0 for the main tree, node)
	vector<std::pair<idx_t, const ExplainTreeNode *>> pending;
};

//===--------------------------------------------------------------------===//
// Explain renderer
//===--------------------------------------------------------------------===//
class ModeExplainRenderer : public RowRenderer {
public:
	//! Several operators are only merged into one "⋯" node (folded runs / collapsed sub-trees) for plans with at least
	//! this many operators; smaller plans keep every operator as its own box.
	static constexpr idx_t MERGE_MIN_NODES = 30;

	explicit ModeExplainRenderer(ShellState &state) : RowRenderer(state) {
	}

	void Analyze(RenderingQueryResult &result) override {
		// materialize the full result and pre-render it - the rendered output is used both for the
		// pager decision and for the actual rendering
		while (result.TryConvertChunk()) {
		}
		if (result.ColumnCount() != 2) {
			return;
		}
		for (auto &chunk : result.chunks) {
			auto &key_vector = chunk->data[0];
			auto &value_vector = chunk->data[1];
			auto key_data = duckdb::FlatVector::GetData<duckdb::string_t>(key_vector);
			auto value_data = duckdb::FlatVector::GetData<duckdb::string_t>(value_vector);
			for (idx_t r = 0; r < chunk->size(); r++) {
				if (duckdb::FlatVector::IsNull(key_vector, r) || duckdb::FlatVector::IsNull(value_vector, r)) {
					continue;
				}
				RenderEntry(key_data[r].GetString(), value_data[r].GetString());
			}
		}
	}

	bool RequireMaterializedResult() const override {
		return true;
	}

	bool ShouldUsePager(RenderingQueryResult &result, PagerMode global_mode) override {
		if (global_mode == PagerMode::PAGER_ON) {
			return true;
		}
		return rendered_lines.size() >= state.pager_min_rows;
	}

	SuccessState RenderQueryResult(PrintStream &out, ShellState &state, RenderingQueryResult &result) override {
		bool highlight = state.HighlightResults() && out.SupportsHighlight();
		ShellHighlight shell_highlight(state);
		for (auto &line : rendered_lines) {
			if (state.seenInterrupt) {
				state.PrintF("Interrupt\n");
				return SuccessState::FAILURE;
			}
			for (auto &span : line) {
				if (!highlight) {
					out.Print(span.text);
				} else if (span.has_color_override) {
					shell_highlight.PrintText(span.text, PrintOutput::STDOUT, span.color, span.intensity);
				} else {
					shell_highlight.PrintText(span.text, PrintOutput::STDOUT, span.type);
				}
			}
			out.Print("\n");
		}
		return SuccessState::SUCCESS;
	}

private:
	void AddLine(ExplainLine line) {
		rendered_lines.push_back(std::move(line));
	}
	void AddText(const string &text, HighlightElementType type = HighlightElementType::NONE) {
		ExplainLine line;
		line.emplace_back(text, type);
		AddLine(std::move(line));
	}

	void RenderEntry(const string &key, const string &value) {
		if (state.pretty_explain && TryRenderPretty(key, value)) {
			return;
		}
		RenderRaw(key, value);
	}

	//! Raw rendering - used when the user explicitly requested an output format (e.g. EXPLAIN (FORMAT JSON))
	void RenderRaw(const string &key, const string &value) {
		RenderHeaderText(key, -1);
		auto lines = StringUtil::Split(value, '\n');
		for (auto &line : lines) {
			AddText(line);
		}
	}

	void RenderHeaderText(const string &key, double total_time) {
		string header;
		if (key == "logical_plan") {
			header = "Unoptimized Logical Plan";
		} else if (key == "logical_opt") {
			header = "Optimized Logical Plan";
		} else if (key == "physical_plan") {
			header = "Physical Plan";
		} else if (key == "analyzed_plan") {
			header = "Query Profile";
		} else {
			return;
		}
		AddText("");
		ExplainLine header_line;
		header_line.emplace_back(header, HighlightElementType::EXPLAIN_HEADER);
		if (total_time >= 0) {
			header_line.emplace_back(" · total time: " + FormatTiming(total_time),
			                         HighlightElementType::EXPLAIN_DETAIL_KEY);
		}
		AddLine(std::move(header_line));
		AddText(StringUtil::Repeat("─", ShellState::RenderLength(header)), HighlightElementType::EXPLAIN_LAYOUT);
	}

	bool TryRenderPretty(const string &key, const string &value) {
		auto doc = yyjson_read(value.c_str(), value.size(), duckdb_yyjson::YYJSON_READ_ALLOW_INF_AND_NAN);
		if (!doc) {
			return false;
		}
		auto root = yyjson_doc_get_root(doc);
		unique_ptr<ExplainTreeNode> plan;
		double total_time = -1;
		if (key == "analyzed_plan") {
			plan = ParseProfilerRoot(root, total_time);
		} else if (yyjson_is_arr(root) && yyjson_arr_size(root) == 1) {
			plan = ParsePlanNode(duckdb_yyjson::yyjson_arr_get_first(root));
		}
		yyjson_doc_free(doc);
		if (!plan) {
			return false;
		}
		ComputeSubtreeStats(*plan);
		string title = HeaderTitle(key);
		string suffix = total_time >= 0 ? " · total time: " + FormatTiming(total_time) : string();
		idx_t render_width = state.GetMaxRenderWidth();
		// left alignment by default (the centered layout is also available, but not yet user-configurable)
		ExplainAlignment alignment = ExplainAlignment::LEFT;
		// condense individual low-impact operators by default whenever we have timing (every EXPLAIN ANALYZE plan)
		bool flatten = total_time > 0;
		// only merge several operators into one "⋯" node (folding runs / collapsing sub-trees) for large plans -
		// small plans keep every operator visible, since there is little clutter to hide when the plan is small
		bool merge = flatten && plan->subtree_count >= MERGE_MIN_NODES;
		ExplainBoxRenderer box_renderer(render_width, state.thousand_separator ? state.thousand_separator : ',',
		                                SumOperatorTimings(*plan), alignment, state.stdout_is_console, flatten, merge);
		for (auto &line : box_renderer.Render(*plan, title, suffix)) {
			AddLine(std::move(line));
		}
		return true;
	}

	static string HeaderTitle(const string &key) {
		if (key == "logical_plan") {
			return "Unoptimized Logical Plan";
		}
		if (key == "logical_opt") {
			return "Optimized Logical Plan";
		}
		if (key == "physical_plan") {
			return "Physical Plan";
		}
		if (key == "analyzed_plan") {
			return "Query Profile";
		}
		return string();
	}

	static unique_ptr<ExplainTreeNode> ParseProfilerRoot(yyjson_val *root, double &total_time) {
		if (!yyjson_is_obj(root)) {
			return nullptr;
		}
		// current metric output format - operators are nested under "operator", query metrics under "query"
		auto op = yyjson_obj_get(root, "operator");
		auto query = yyjson_obj_get(root, "query");
		if (yyjson_is_obj(query)) {
			auto query_time = yyjson_obj_get(query, "total_time");
			if (yyjson_is_num(query_time)) {
				total_time = yyjson_get_num(query_time);
			}
		}
		if (!op) {
			// legacy metric output format - the root operator is nested under "children"
			op = yyjson_obj_get(root, "children");
			auto latency = yyjson_obj_get(root, "latency");
			if (yyjson_is_num(latency)) {
				total_time = yyjson_get_num(latency);
			}
		}
		if (!yyjson_is_arr(op) || yyjson_arr_size(op) != 1) {
			return nullptr;
		}
		auto plan = ParseProfilerNode(duckdb_yyjson::yyjson_arr_get_first(op));
		// skip the EXPLAIN_ANALYZE / RESULT_COLLECTOR wrapper operators
		while (plan && (plan->name == "EXPLAIN_ANALYZE" || plan->name == "RESULT_COLLECTOR") &&
		       plan->children.size() == 1) {
			plan = std::move(plan->children[0]);
		}
		return plan;
	}

private:
	vector<ExplainLine> rendered_lines;
};

unique_ptr<ShellRenderer> CreateExplainRenderer(ShellState &state) {
	return make_uniq<ModeExplainRenderer>(state);
}

} // namespace duckdb_shell
