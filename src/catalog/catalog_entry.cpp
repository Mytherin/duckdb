#include "duckdb/catalog/catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"

namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, Identifier name_p, idx_t oid)
    : oid(oid), type(type), set(nullptr), name(std::move(name_p)), deleted(false), temporary(false), internal(false),
      parent(nullptr) {
}

static idx_t ResolveCatalogEntryOid(Catalog &catalog, optional_idx oid) {
	auto &db_manager = catalog.GetDatabase().GetDatabaseManager();
	if (oid.IsValid()) {
		// the entry has a persisted oid - reuse it, and make sure future oids never collide with it
		db_manager.ReseedNextOid(oid.GetIndex() + 1);
		return oid.GetIndex();
	}
	return db_manager.NextOid();
}

CatalogEntry::CatalogEntry(CatalogType type, Catalog &catalog, Identifier name_p, optional_idx oid)
    : CatalogEntry(type, std::move(name_p), ResolveCatalogEntryOid(catalog, oid)) {
}

CatalogEntry::~CatalogEntry() {
}

void CatalogEntry::SetAsRoot() {
}

// LCOV_EXCL_START
unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	throw InternalException("Unsupported alter type for catalog entry!");
}

unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(CatalogTransaction transaction, AlterInfo &info) {
	if (!transaction.context) {
		throw InternalException("Cannot AlterEntry without client context");
	}
	return AlterEntry(*transaction.context, info);
}

void CatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) const {
	throw InternalException("Unsupported copy type for catalog entry!");
}

unique_ptr<CreateInfo> CatalogEntry::GetInfo() const {
	throw InternalException("Unsupported type for CatalogEntry::GetInfo!");
}

string CatalogEntry::ToSQL() const {
	throw InternalException({{"catalog_type", CatalogTypeToString(type)}}, "Unsupported catalog type for ToSQL()");
}

void CatalogEntry::SetChild(unique_ptr<CatalogEntry> child_p) {
	child = std::move(child_p);
	if (child) {
		child->parent.store(this);
	}
}

unique_ptr<CatalogEntry> CatalogEntry::TakeChild() {
	if (child) {
		child->parent.store(nullptr);
	}
	return std::move(child);
}

bool CatalogEntry::HasChild() const {
	return child != nullptr;
}
bool CatalogEntry::HasParent() const {
	return parent.load() != nullptr;
}

CatalogEntry &CatalogEntry::Child() {
	return *child;
}

CatalogEntry &CatalogEntry::Parent() {
	return *parent.load();
}

const CatalogEntry &CatalogEntry::Parent() const {
	return *parent.load();
}

Catalog &CatalogEntry::ParentCatalog() {
	throw InternalException("CatalogEntry::ParentCatalog called on catalog entry without catalog");
}

const Catalog &CatalogEntry::ParentCatalog() const {
	throw InternalException("CatalogEntry::ParentCatalog called on catalog entry without catalog");
}

SchemaCatalogEntry &CatalogEntry::ParentSchema() {
	throw InternalException("CatalogEntry::ParentSchema called on catalog entry without schema");
}

const SchemaCatalogEntry &CatalogEntry::ParentSchema() const {
	throw InternalException("CatalogEntry::ParentSchema called on catalog entry without schema");
}
// LCOV_EXCL_STOP

void CatalogEntry::Serialize(Serializer &serializer) const {
	const auto info = GetInfo();
	info->Serialize(serializer);
}

unique_ptr<CreateInfo> CatalogEntry::Deserialize(Deserializer &deserializer) {
	return CreateInfo::Deserialize(deserializer);
}

void CatalogEntry::Verify(Catalog &catalog_p) {
}

void CatalogEntry::Rollback(CatalogEntry &prev_entry) {
}

void CatalogEntry::OnDrop() {
}

InCatalogEntry::InCatalogEntry(CatalogType type, Catalog &catalog, Identifier name, optional_idx oid)
    : CatalogEntry(type, catalog, std::move(name), oid), catalog(catalog) {
}

InCatalogEntry::~InCatalogEntry() {
}

void InCatalogEntry::Verify(Catalog &catalog_p) {
	D_ASSERT(&catalog_p == &catalog);
}
} // namespace duckdb
