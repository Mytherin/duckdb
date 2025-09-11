#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/standard_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

BoundStatement Binder::Bind(DropStatement &stmt) {
	BoundStatement result;

	auto &properties = GetStatementProperties();
	switch (stmt.info->type) {
	case CatalogType::PREPARED_STATEMENT:
		// dropping prepared statements is always possible
		// it also does not require a valid transaction
		properties.requires_valid_transaction = false;
		break;
	case CatalogType::SCHEMA_ENTRY: {
		// dropping a schema is never read-only because there are no temporary schemas
		auto &catalog = Catalog::GetCatalog(context, stmt.info->catalog);
		properties.RegisterDBModify(catalog, context);
		break;
	}
	case CatalogType::VIEW_ENTRY:
	case CatalogType::SEQUENCE_ENTRY:
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::INDEX_ENTRY:
	case CatalogType::TABLE_ENTRY:
	case CatalogType::TYPE_ENTRY: {
		BindSchemaOrCatalog(stmt.info->catalog, stmt.info->schema);
		auto catalog = Catalog::GetCatalogEntry(context, stmt.info->catalog);
		if (catalog) {
			// mark catalog as accessed
			properties.RegisterDBRead(*catalog, context);
		}
		auto on_not_found = stmt.info->if_not_found;
		bool is_macro =
		    stmt.info->type == CatalogType::MACRO_ENTRY || stmt.info->type == CatalogType::TABLE_MACRO_ENTRY;
		if (is_macro && on_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			on_not_found = OnEntryNotFound::RETURN_NULL;
		}
		EntryLookupInfo entry_lookup(stmt.info->type, stmt.info->name);
		auto entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, entry_lookup, on_not_found);
		if (!entry) {
			if (is_macro && stmt.info->if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
				// we are trying to drop either a macro or a table macro, but none was found
				// check if the other option exists
				bool is_scalar_macro = stmt.info->type == CatalogType::MACRO_ENTRY;
				auto other_macro_type = is_scalar_macro ? CatalogType::TABLE_MACRO_ENTRY : CatalogType::MACRO_ENTRY;
				EntryLookupInfo other_lookup(other_macro_type, stmt.info->name);
				entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, other_lookup,
				                          OnEntryNotFound::RETURN_NULL);
				if (entry) {
					// we found the other entry - suggest the other drop method
					throw CatalogException("%s with name \"%s\" does not exist - but it exists as a %s\nDid you mean "
					                       "to use \"DROP MACRO %s%s\"?",
					                       CatalogTypeToString(stmt.info->type), stmt.info->name,
					                       CatalogTypeToString(other_macro_type), is_scalar_macro ? "TABLE " : "",
					                       stmt.info->name);
				}
				// also does not exist as the other macro type - perform the original look-up to throw the exception
				entry = Catalog::GetEntry(context, stmt.info->catalog, stmt.info->schema, entry_lookup,
				                          OnEntryNotFound::THROW_EXCEPTION);
			}
			break;
		}
		if (entry->internal) {
			throw CatalogException("Cannot drop internal catalog entry \"%s\"!", entry->name);
		}
		stmt.info->catalog = entry->ParentCatalog().GetName();
		if (!entry->temporary) {
			// we can only drop temporary schema entries in read-only mode
			properties.RegisterDBModify(entry->ParentCatalog(), context);
		}
		stmt.info->schema = entry->ParentSchema().name;
		break;
	}
	case CatalogType::SECRET_ENTRY: {
		//! Secrets are stored in the secret manager; they can always be dropped
		properties.requires_valid_transaction = false;
		break;
	}
	default:
		throw BinderException("Unknown catalog type for drop statement: '%s'", CatalogTypeToString(stmt.info->type));
	}
	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_DROP, std::move(stmt.info));
	result.names = {"Success"};
	result.types = {LogicalType::BOOLEAN};

	properties.allow_stream_result = false;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
