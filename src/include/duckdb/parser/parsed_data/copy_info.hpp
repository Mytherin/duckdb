//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/copy_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {

class QueryNode;

struct CopyInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::COPY_INFO;

public:
	CopyInfo();

	//! The table name to copy to/from
	Identifier table;
	//! List of columns to copy to/from
	vector<Identifier> select_list;
	//! Whether or not this is a copy to file (false) or copy from a file (true)
	bool is_from;
	//! The file format of the external file
	string format;
	//! If the format is manually set (i.e., via the format parameter) or was discovered by inspecting the file path
	bool is_format_auto_detected;
	//! Expression to determine the file path (if any)
	unique_ptr<ParsedExpression> file_path_expression;
	//! The file path to copy to/from
	string file_path;
	//! Set of (key, value) options
	case_insensitive_map_t<unique_ptr<ParsedExpression>> parsed_options;
	//! Set of (key, value) options
	case_insensitive_map_t<vector<Value>> options;
	//! The SQL statement used instead of a table when copying data out to a file
	unique_ptr<QueryNode> select_statement;

public:
	//! The catalog is only set when fully qualified, i.e. schema_path holds [catalog, schema]
	const Identifier &GetCatalog() const {
		return schema_path.size() >= 2 ? schema_path[0] : Identifier::Empty();
	}
	//! The schema is the last element of the qualification path (empty if the path is empty)
	const Identifier &GetSchema() const {
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

	string CopyOptionsToString() const;

public:
	unique_ptr<CopyInfo> Copy() const;
	string ToString() const;
	string TablePartToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

private:
	//! Qualification path: element 0 is the catalog (when present), the remainder are schema levels.
	//! Today this holds at most [catalog, schema]; (catalog, schema) is derived following the size rules above.
	vector<Identifier> schema_path;
};

} // namespace duckdb
