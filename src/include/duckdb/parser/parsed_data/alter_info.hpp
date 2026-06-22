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
#include "duckdb/parser/qualified_name.hpp"
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
	AlterEntryData(QualifiedName name_p, OnEntryNotFound if_not_found)
	    : name(std::move(name_p)), if_not_found(if_not_found) {
	}

	//! The (optionally qualified) name of the entry to alter
	QualifiedName name;
	OnEntryNotFound if_not_found;
};

struct AlterInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::ALTER_INFO;

public:
	AlterInfo(AlterType type, QualifiedName name, OnEntryNotFound if_not_found);
	AlterInfo(AlterType type, Identifier catalog, Identifier schema, Identifier name, OnEntryNotFound if_not_found);
	AlterInfo(AlterType type, vector<Identifier> schema_path, Identifier name, OnEntryNotFound if_not_found);
	~AlterInfo() override;

	AlterType type;
	//! if exists
	OnEntryNotFound if_not_found;
	//! Entry name to alter (the bare name plus its catalog/schema qualification path)
	QualifiedName name;
	//! Allow altering internal entries
	bool allow_internal;
	//! Determine whether to skip Bind
	AlterBindMode bind_mode = AlterBindMode::BIND_ON_ALTER;
	//! New dependencies for the altered entry (set during binding)
	unique_ptr<LogicalDependencyList> new_dependencies;

public:
	const Identifier &GetCatalog() const {
		return name.GetCatalog();
	}
	const Identifier &GetSchema() const {
		return name.GetSchema();
	}
	const vector<Identifier> &GetSchemaPath() const {
		return name.GetSchemaPath();
	}
	//! Assign the full qualified name
	void SetQualifiedName(QualifiedName name_p) {
		name = std::move(name_p);
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
};

} // namespace duckdb
