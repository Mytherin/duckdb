//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/enums/on_create_conflict.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/catalog/dependency_list.hpp"

namespace duckdb {
struct AlterInfo;

struct CreateInfo : public ParseInfo {
public:
	static constexpr const ParseInfoType TYPE = ParseInfoType::CREATE_INFO;

public:
	explicit CreateInfo(CatalogType type, Identifier schema = Identifier::DefaultSchema(),
	                    Identifier catalog = Identifier::InvalidCatalog());
	CreateInfo(CatalogType type, vector<Identifier> schema_path);
	~CreateInfo() override {
	}

	//! The to-be-created catalog type
	CatalogType type;

	//! The (optionally qualified) name of the entry being created - the bare name plus its catalog/schema path.
	//! The bare name part is owned/managed by the concrete subclass (via its semantic accessor).
	QualifiedName name;

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
	//! The bare entry name (without qualification)
	const Identifier &GetEntryName() const {
		return name.name;
	}

public:
	//! What to do on create conflict
	OnCreateConflict on_conflict;
	//! Whether or not the entry is temporary
	bool temporary;
	//! Whether or not the entry is an internal entry
	bool internal;
	//! The name of the extension that registered this entry (empty for core entries)
	Identifier extension_name;
	//! The SQL string of the CREATE statement
	string sql;
	//! The inherent dependencies of the created entry
	LogicalDependencyList dependencies;
	//! User provided comment
	Value comment;
	//! Key-value tags with additional metadata
	InsertionOrderPreservingMap<string> tags;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<CreateInfo> Deserialize(Deserializer &deserializer);

	virtual unique_ptr<CreateInfo> Copy() const = 0;

	DUCKDB_API void CopyProperties(CreateInfo &other) const;
	//! Generates an alter statement from the create statement - used for OnCreateConflict::ALTER_ON_CONFLICT
	DUCKDB_API virtual unique_ptr<AlterInfo> GetAlterInfo() const;
	//! Returns a string like "CREATE (OR REPLACE) (TEMPORARY) <entry> (IF NOT EXISTS) " for TABLE/VIEW/TYPE/MACRO
	DUCKDB_API string GetCreatePrefix(const string &entry) const;

	virtual string ToString() const {
		throw NotImplementedException("ToString not supported for this type of CreateInfo: '%s'",
		                              EnumUtil::ToString(info_type));
	}
};

} // namespace duckdb
