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
#include "duckdb/parser/qualified_name.hpp"
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
	//! Element name to drop (the bare name plus its catalog/schema qualification path)
	QualifiedName name;
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

	virtual unique_ptr<DropInfo> Copy() const;
	string ToString() const;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<ParseInfo> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
