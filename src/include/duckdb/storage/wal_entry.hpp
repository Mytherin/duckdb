//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/wal_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/wal_type.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/index_storage_info.hpp"
#include "duckdb/storage/table/column_data.hpp"

namespace duckdb {
class Serializer;
class Deserializer;
struct AlterInfo;

//===--------------------------------------------------------------------===//
// WAL entries
//===--------------------------------------------------------------------===//
// A WALEntry holds the payload of a single Write-Ahead Log entry. The polymorphic base writes/reads the WALType
// marker (field id 100) and dispatches to the concrete subclass. (De)serialization of the simple entries is
// generated from storage/serialization/wal.json; the data-heavy entries provide hand-written implementations. The
// WAL replay logic (which performs the catalog/storage side effects) operates on the deserialized entries.

struct WALEntry {
	explicit WALEntry(WALType wal_type) : wal_type(wal_type) {
	}
	virtual ~WALEntry() = default;

	WALType wal_type;

	virtual void Serialize(Serializer &serializer) const;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//===--------------------------------------------------------------------===//
// Metadata
//===--------------------------------------------------------------------===//
struct WALVersion : WALEntry {
	WALVersion() : WALEntry(WALType::WAL_VERSION) {
	}

	idx_t version = 0;
	//! The matching database file identifier (empty if not written - i.e. pre-v1.3.0 storage)
	vector<data_t> db_identifier;
	optional_idx checkpoint_iteration;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCheckpoint : WALEntry {
	WALCheckpoint() : WALEntry(WALType::CHECKPOINT) {
	}

	MetaBlockPointer meta_block;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALFlush : WALEntry {
	WALFlush() : WALEntry(WALType::WAL_FLUSH) {
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

//===--------------------------------------------------------------------===//
// Catalog
//===--------------------------------------------------------------------===//
struct WALCreateTable : WALEntry {
	WALCreateTable() : WALEntry(WALType::CREATE_TABLE) {
	}

	unique_ptr<CreateInfo> table;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropTable : WALEntry {
	WALDropTable() : WALEntry(WALType::DROP_TABLE) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateSchema : WALEntry {
	WALCreateSchema() : WALEntry(WALType::CREATE_SCHEMA) {
	}

	Identifier schema;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropSchema : WALEntry {
	WALDropSchema() : WALEntry(WALType::DROP_SCHEMA) {
	}

	Identifier schema;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateView : WALEntry {
	WALCreateView() : WALEntry(WALType::CREATE_VIEW) {
	}

	unique_ptr<CreateInfo> view;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropView : WALEntry {
	WALDropView() : WALEntry(WALType::DROP_VIEW) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateSequence : WALEntry {
	WALCreateSequence() : WALEntry(WALType::CREATE_SEQUENCE) {
	}

	unique_ptr<CreateInfo> sequence;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropSequence : WALEntry {
	WALDropSequence() : WALEntry(WALType::DROP_SEQUENCE) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALSequenceValue : WALEntry {
	WALSequenceValue() : WALEntry(WALType::SEQUENCE_VALUE) {
	}

	Identifier schema;
	Identifier name;
	uint64_t usage_count = 0;
	int64_t counter = 0;
	optional<int64_t> last_value;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateMacro : WALEntry {
	WALCreateMacro() : WALEntry(WALType::CREATE_MACRO) {
	}

	unique_ptr<CreateInfo> macro;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropMacro : WALEntry {
	WALDropMacro() : WALEntry(WALType::DROP_MACRO) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateTableMacro : WALEntry {
	WALCreateTableMacro() : WALEntry(WALType::CREATE_TABLE_MACRO) {
	}

	unique_ptr<CreateInfo> table_macro;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropTableMacro : WALEntry {
	WALDropTableMacro() : WALEntry(WALType::DROP_TABLE_MACRO) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateType : WALEntry {
	WALCreateType() : WALEntry(WALType::CREATE_TYPE) {
	}

	unique_ptr<CreateInfo> type;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropType : WALEntry {
	WALDropType() : WALEntry(WALType::DROP_TYPE) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALCreateTrigger : WALEntry {
	WALCreateTrigger() : WALEntry(WALType::CREATE_TRIGGER) {
	}

	unique_ptr<CreateInfo> trigger;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropTrigger : WALEntry {
	WALDropTrigger() : WALEntry(WALType::DROP_TRIGGER) {
	}

	Identifier schema;
	Identifier name;
	Identifier table;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
struct WALCreateIndex : WALEntry {
	WALCreateIndex() : WALEntry(WALType::CREATE_INDEX) {
	}

	unique_ptr<CreateInfo> index_catalog_entry;
	bool has_index_storage = false;
	IndexStorageInfo index_storage_info;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDropIndex : WALEntry {
	WALDropIndex() : WALEntry(WALType::DROP_INDEX) {
	}

	Identifier schema;
	Identifier name;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

//===--------------------------------------------------------------------===//
// ALTER
//===--------------------------------------------------------------------===//
struct WALAlter : WALEntry {
	WALAlter() : WALEntry(WALType::ALTER_INFO) {
	}

	//! Non-owning view used for serialization (the AlterInfo is owned by the caller of WriteAlter)
	optional_ptr<const AlterInfo> write_info;
	//! Owned info produced by deserialization / consumed by replay
	unique_ptr<ParseInfo> info;
	//! Index data for an ADD PRIMARY KEY alter (only valid when info is an AddConstraintInfo adding a primary key)
	bool has_index_storage = false;
	IndexStorageInfo index_storage_info;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

//===--------------------------------------------------------------------===//
// Data
//===--------------------------------------------------------------------===//
struct WALUseTable : WALEntry {
	WALUseTable() : WALEntry(WALType::USE_TABLE) {
	}

	Identifier schema;
	Identifier table;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALInsert : WALEntry {
	WALInsert() : WALEntry(WALType::INSERT_TUPLE) {
	}

	//! Non-owning view used for serialization (the chunk is owned by the caller of WriteInsert)
	optional_ptr<DataChunk> write_chunk;
	//! Owned chunk produced by deserialization / consumed by replay
	DataChunk chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALDelete : WALEntry {
	WALDelete() : WALEntry(WALType::DELETE_TUPLE) {
	}

	//! Non-owning view used for serialization (the chunk is owned by the caller of WriteDelete)
	optional_ptr<DataChunk> write_chunk;
	//! Owned chunk produced by deserialization / consumed by replay
	DataChunk chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALUpdate : WALEntry {
	WALUpdate() : WALEntry(WALType::UPDATE_TUPLE) {
	}

	vector<column_t> column_indexes;
	//! Non-owning view used for serialization (the chunk is owned by the caller of WriteUpdate)
	optional_ptr<DataChunk> write_chunk;
	//! Owned chunk produced by deserialization / consumed by replay
	DataChunk chunk;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

struct WALRowGroupData : WALEntry {
	WALRowGroupData() : WALEntry(WALType::ROW_GROUP_DATA) {
	}

	//! Non-owning view used for serialization (the data is owned by the caller of WriteRowGroupData)
	optional_ptr<const PersistentCollectionData> write_data;
	//! Owned data produced by deserialization / consumed by replay
	PersistentCollectionData row_group_data;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<WALEntry> Deserialize(Deserializer &deserializer);
};

} // namespace duckdb
