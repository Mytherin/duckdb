//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/drop_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/parsed_data/extra_drop_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

namespace duckdb {
struct ExtraDropInfo;

struct DropInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::DROP_INFO;

public:
	DropInfo();
	DropInfo(const DropInfo &info);

	//! The catalog type to drop
	CatalogType type;
	//! Element name to drop
	Identifier name;
	//! Ignore if the entry does not exist instead of failing
	OnEntryNotFound if_not_found = OnEntryNotFound::THROW_EXCEPTION;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;
	//! Allow dropping of internal system entries
	bool allow_drop_internal = false;
	//! Extra info related to this drop
	unique_ptr<ExtraDropInfo> extra_drop_info;

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

	virtual unique_ptr<DropInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

private:
	//! Qualification path: element 0 is the catalog (when present), the remainder are schema levels.
	//! Today this holds at most [catalog, schema]; (catalog, schema) is derived following the size rules above.
	vector<Identifier> schema_path;
};

} // namespace duckdb
