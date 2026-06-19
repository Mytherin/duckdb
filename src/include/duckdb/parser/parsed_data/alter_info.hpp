//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {

enum class AlterType : uint8_t {
	INVALID = 0,
	ALTER_TABLE = 1,
	ALTER_VIEW = 2,
	ALTER_SEQUENCE = 3,
	CHANGE_OWNERSHIP = 4,
	ALTER_SCALAR_FUNCTION = 5,
	ALTER_TABLE_FUNCTION = 6,
	SET_COMMENT = 7,
	SET_COLUMN_COMMENT = 8,
	ALTER_DATABASE = 9
};

enum class AlterBindMode { BIND_ON_ALTER, SKIP_BINDING };

struct AlterEntryData {
	AlterEntryData() {
	}
	AlterEntryData(Identifier catalog_p, Identifier schema_p, Identifier name_p, OnEntryNotFound if_not_found)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), name(std::move(name_p)),
	      if_not_found(if_not_found) {
	}

	Identifier catalog;
	Identifier schema;
	Identifier name;
	OnEntryNotFound if_not_found;
};

struct AlterInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ALTER_INFO;

public:
	AlterInfo(AlterType type, Identifier catalog, Identifier schema, Identifier name, OnEntryNotFound if_not_found);
	~AlterInfo() override;

	AlterType type;
	//! if exists
	OnEntryNotFound if_not_found;
	//! Entry name to alter
	Identifier name;
	//! Allow altering internal entries
	bool allow_internal;
	//! Determine whether to skip Bind
	AlterBindMode bind_mode = AlterBindMode::BIND_ON_ALTER;
	//! New dependencies for the altered entry (set during binding)
	unique_ptr<LogicalDependencyList> new_dependencies;

public:
	//! The catalog is only set when fully qualified, i.e. schema_path holds [catalog, schema]
	const Identifier &GetCatalog() const {
		return schema_path.size() >= 2 ? schema_path[0] : EmptyIdentifier();
	}
	//! The schema is the last element of the qualification path (empty if the path is empty)
	const Identifier &GetSchema() const {
		if (schema_path.size() == 1) {
			return schema_path[0];
		}
		if (schema_path.size() >= 2) {
			return schema_path[1];
		}
		return EmptyIdentifier();
	}
	void SetCatalog(Identifier catalog_p);
	void SetSchema(Identifier schema_p);

	const vector<Identifier> &GetSchemaPath() const {
		return schema_path;
	}

	virtual CatalogType GetCatalogType() const = 0;
	virtual unique_ptr<AlterInfo> Copy() const = 0;
	virtual string ToString() const = 0;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);

	virtual Identifier GetColumnName() const {
		return Identifier();
	};

	AlterEntryData GetAlterEntryData() const;
	bool IsAddPrimaryKey() const;

protected:
	explicit AlterInfo(AlterType type);

private:
	static const Identifier &EmptyIdentifier();

private:
	//! Qualification path: element 0 is the catalog (when present), the remainder are schema levels.
	//! Today this holds at most [catalog, schema]; (catalog, schema) is derived following the size rules above.
	vector<Identifier> schema_path;
};

} // namespace duckdb
