#include "duckdb/storage/write_ahead_log.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/scalar_macro_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/common/checksum.hpp"
#include "duckdb/common/encryption_functions.hpp"
#include "duckdb/common/encryption_key_manager.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/wal_entry.hpp"

namespace duckdb {

constexpr uint64_t WAL_VERSION_NUMBER = 2;
constexpr uint64_t WAL_ENCRYPTED_VERSION_NUMBER = 3;

WriteAheadLog::WriteAheadLog(StorageManager &storage_manager, const string &wal_path, idx_t wal_size,
                             WALInitState init_state, optional_idx checkpoint_iteration)
    : storage_manager(storage_manager), wal_path(wal_path), init_state(init_state),
      checkpoint_iteration(checkpoint_iteration) {
	storage_manager.SetWALSize(wal_size);
	storage_manager.ResetWALEntriesCount();
}

WriteAheadLog::~WriteAheadLog() {
}

AttachedDatabase &WriteAheadLog::GetDatabase() {
	return storage_manager.GetAttached();
}

StorageManager &WriteAheadLog::GetStorageManager() {
	return storage_manager;
}

BufferedFileWriter &WriteAheadLog::Initialize() {
	if (Initialized()) {
		return *writer;
	}
	lock_guard<mutex> lock(wal_lock);
	if (!writer) {
		writer =
		    make_uniq<BufferedFileWriter>(FileSystem::Get(GetDatabase()), wal_path,
		                                  FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE |
		                                      FileFlags::FILE_FLAGS_APPEND | FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS);
		if (init_state == WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE) {
			writer->Truncate(storage_manager.GetWALSize());
		} else {
			storage_manager.SetWALSize(writer->GetFileSize());
		}
		init_state = WALInitState::INITIALIZED;
	}
	return *writer;
}

idx_t WriteAheadLog::GetTotalWritten() const {
	if (!Initialized()) {
		return 0;
	}
	return writer->GetTotalWritten();
}

void WriteAheadLog::Truncate(idx_t size) {
	if (init_state == WALInitState::NO_WAL) {
		// no WAL to truncate
		return;
	}
	if (!Initialized()) {
		init_state = WALInitState::UNINITIALIZED_REQUIRES_TRUNCATE;
		storage_manager.SetWALSize(size);
		return;
	}
	writer->Truncate(size);
	storage_manager.SetWALSize(writer->GetFileSize());
}

bool WriteAheadLog::Initialized() const {
	return init_state == WALInitState::INITIALIZED;
}

//===--------------------------------------------------------------------===//
// Serializer
//===--------------------------------------------------------------------===//
class ChecksumWriter : public WriteStream {
public:
	explicit ChecksumWriter(WriteAheadLog &wal) : wal(wal), memory_stream(Allocator::Get(wal.GetDatabase())) {
	}

	void WriteData(const_data_ptr_t buffer, idx_t write_size) override {
		// buffer data into the memory stream
		memory_stream.WriteData(buffer, write_size);
	}

	void Flush() {
		if (!stream) {
			stream = wal.Initialize();
		}

		// if the config.encrypt WAL is true
		// and if the attached database is encrypted
		// then encrypt WAL before flushing
		auto &catalog = wal.GetDatabase().GetCatalog().Cast<DuckCatalog>();

		if (catalog.GetIsEncrypted()) {
			return FlushEncrypted();
		}

		auto data = memory_stream.GetData();
		auto size = memory_stream.GetPosition();
		// compute the checksum over the entry
		auto checksum = Checksum(data, size);
		// write the checksum and the length of the entry
		stream->Write<uint64_t>(size);
		stream->Write<uint64_t>(checksum);
		// write data to the underlying stream
		stream->WriteData(memory_stream.GetData(), memory_stream.GetPosition());
		// rewind the buffer
		memory_stream.Rewind();
	}

	void FlushEncrypted() {
		auto &catalog = wal.GetDatabase().GetCatalog().Cast<DuckCatalog>();
		auto encryption_key_id = catalog.GetEncryptionKeyId();

		auto data = memory_stream.GetData();
		auto size = memory_stream.GetPosition();

		// compute the checksum over the entry
		auto checksum = Checksum(data, size);

		auto &db = wal.GetDatabase();
		auto &keys = EncryptionKeyManager::Get(db.GetDatabase());
		auto metadata = make_uniq<EncryptionStateMetadata>(db.GetStorageManager().GetCipher(),
		                                                   MainHeader::DEFAULT_ENCRYPTION_KEY_LENGTH,
		                                                   EncryptionTypes::EncryptionVersion::V0_1);
		auto encryption_state =
		    db.GetDatabase().GetEncryptionUtil(db.IsReadOnly())->CreateEncryptionState(std::move(metadata));

		// temp buffer
		const idx_t ciphertext_size = size + sizeof(uint64_t);
		std::unique_ptr<uint8_t[]> temp_buf(new uint8_t[ciphertext_size]);

		EncryptionNonce nonce(db.GetStorageManager().GetCipher(), db.GetStorageManager().GetEncryptionVersion());
		EncryptionTag tag;

		// generate nonce
		encryption_state->GenerateRandomData(nonce.data(), nonce.size());

		stream->Write<uint64_t>(size);
		stream->WriteData(nonce.data(), nonce.size());

		//! store the checksum in the temp buffer
		memcpy(temp_buf.get(), &checksum, sizeof(checksum));
		//! checksum + entry in the temp buf
		memcpy(temp_buf.get() + sizeof(checksum), memory_stream.GetData(), memory_stream.GetPosition());

		//! encrypt the temp buf
		encryption_state->InitializeEncryption(nonce, keys.GetKey(encryption_key_id));
		encryption_state->Process(temp_buf.get(), ciphertext_size, temp_buf.get(), ciphertext_size);

		//! calculate the tag (for GCM)
		encryption_state->Finalize(temp_buf.get(), ciphertext_size, tag.data(), tag.size());

		// write data to the underlying stream
		stream->WriteData(temp_buf.get(), ciphertext_size);

		// Write the tag to the stream
		if (encryption_state->GetCipher() == EncryptionTypes::CipherType::GCM) {
			D_ASSERT(!tag.IsAllZeros());
			stream->WriteData(tag.data(), tag.size());
		}

		// rewind the buffer
		memory_stream.Rewind();
	}

	WriteAheadLog &GetWAL() {
		return wal;
	}

private:
	WriteAheadLog &wal;
	optional_ptr<WriteStream> stream;
	MemoryStream memory_stream;
};

class WriteAheadLogSerializer {
public:
	explicit WriteAheadLogSerializer(WriteAheadLog &wal)
	    : checksum_writer(wal), serializer(checksum_writer, SerializationOptions(wal.GetDatabase())) {
		if (!wal.Initialized()) {
			wal.Initialize();
		}
		// Write a header, if none has been written yet.
		wal.WriteHeader();
		serializer.Begin();
	}

	void End() {
		serializer.End();
		checksum_writer.Flush();
		checksum_writer.GetWAL().IncrementWALEntriesCount();
	}

	//! Serialize a WAL entry (the WALType marker and payload are written by the entry itself)
	void WriteEntry(const WALEntry &entry) {
		entry.Serialize(serializer);
	}

private:
	ChecksumWriter checksum_writer;
	BinarySerializer serializer;
};

void WriteAheadLog::WriteEntry(const WALEntry &entry) {
	WriteAheadLogSerializer serializer(*this);
	serializer.WriteEntry(entry);
	serializer.End();
}

//===--------------------------------------------------------------------===//
// Write Entries
//===--------------------------------------------------------------------===//
void WALVersion::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "version", version);
	// The db_identifier / checkpoint_iteration are only present from v1.3.0 storage onwards.
	if (!db_identifier.empty()) {
		serializer.WriteList(102, "db_identifier", db_identifier.size(),
		                     [&](Serializer::List &list, idx_t i) { list.WriteElement(db_identifier[i]); });
		serializer.WriteProperty(103, "checkpoint_iteration", checkpoint_iteration.GetIndex());
	}
}

void WriteAheadLog::WriteHeader() {
	D_ASSERT(writer);
	if (writer->GetFileSize() > 0) {
		// Already written - no need to write a header.
		return;
	}

	// Write the header containing
	// - the version marker,
	// - the header_id of the matching database file, and
	// - the checkpoint iteration of the matching database file.
	// Note that we explicitly do not checksum the header, as it contains the version entry.

	WALVersion entry;
	auto &database = GetDatabase();
	auto &catalog = database.GetCatalog().Cast<DuckCatalog>();
	entry.version = catalog.GetIsEncrypted() ? idx_t(WAL_ENCRYPTED_VERSION_NUMBER) : idx_t(WAL_VERSION_NUMBER);

	auto &single_file_block_manager = database.GetStorageManager().GetBlockManager().Cast<SingleFileBlockManager>();
	auto file_version_number = single_file_block_manager.GetVersionNumber();
	// double check
	if (StorageManager::TargetAtLeastVersion(StorageVersion::V1_3_0, file_version_number)) {
		auto db_identifier = single_file_block_manager.GetDBIdentifier();
		entry.db_identifier.assign(db_identifier, db_identifier + MainHeader::DB_IDENTIFIER_LEN);
		if (checkpoint_iteration.IsValid()) {
			entry.checkpoint_iteration = checkpoint_iteration.GetIndex();
		} else {
			entry.checkpoint_iteration = single_file_block_manager.GetCheckpointIteration();
		}
	}

	BinarySerializer serializer(*writer);
	serializer.Begin();
	entry.Serialize(serializer);
	serializer.End();
}

void WriteAheadLog::WriteCheckpoint(MetaBlockPointer meta_block) {
	WALCheckpoint entry;
	entry.meta_block = meta_block;
	WriteEntry(entry);
}

//===--------------------------------------------------------------------===//
// CREATE TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTable(const TableCatalogEntry &entry) {
	WALCreateTable wal_entry;
	wal_entry.table = entry.GetInfo();
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// DROP TABLE
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropTable(const TableCatalogEntry &entry) {
	WALDropTable wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// CREATE SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSchema(const SchemaCatalogEntry &entry) {
	WALCreateSchema wal_entry;
	wal_entry.schema = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// SEQUENCES
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateSequence(const SequenceCatalogEntry &entry) {
	WALCreateSequence wal_entry;
	wal_entry.sequence = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropSequence(const SequenceCatalogEntry &entry) {
	WALDropSequence wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteSequenceValue(SequenceValue val) {
	auto &sequence = *val.entry;
	WALSequenceValue wal_entry;
	wal_entry.schema = sequence.schema.name;
	wal_entry.name = sequence.name;
	wal_entry.usage_count = val.usage_count;
	wal_entry.counter = val.counter;
	// last_value is only serialized from v2.0.0 storage onwards (handled by the generated Serialize)
	wal_entry.last_value = val.entry->GetData().last_value;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// MACROS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateMacro(const ScalarMacroCatalogEntry &entry) {
	WALCreateMacro wal_entry;
	wal_entry.macro = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropMacro(const ScalarMacroCatalogEntry &entry) {
	WALDropMacro wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteCreateTableMacro(const TableMacroCatalogEntry &entry) {
	WALCreateTableMacro wal_entry;
	wal_entry.table_macro = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropTableMacro(const TableMacroCatalogEntry &entry) {
	WALDropTableMacro wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// Indexes
//===--------------------------------------------------------------------===//
//! Find the named (bound) index in the list and copy its WAL storage info (metadata + buffers) into result.
//! Returns false if the index could not be found.
static bool GetIndexStorageInfo(AttachedDatabase &db, TableIndexList &list, const Identifier &name,
                                IndexStorageInfo &result) {
	case_insensitive_map_t<Value> options;
	auto storage_version = db.GetStorageManager().GetStorageVersion();
	// Before: serialization version 3
	auto v1_0_0_storage = StorageManager::IsPriorToVersion(StorageVersion::V1_2_0, storage_version);
	if (!v1_0_0_storage) {
		options["v1_0_0_storage"] = v1_0_0_storage;
	}

	for (auto &index : list.Indexes()) {
		if (name == index.GetIndexName()) {
			// We never write an unbound index to the WAL.
			D_ASSERT(index.IsBound());
			result = index.Cast<BoundIndex>().SerializeToWAL(options);
			return true;
		}
	}
	return false;
}

//! Serialize the index storage metadata (id 102) and the raw index buffers (id 103).
static void SerializeIndexStorage(Serializer &serializer, const IndexStorageInfo &info) {
	serializer.WriteProperty(102, "index_storage_info", info);
	serializer.WriteList(103, "index_storage", info.buffers.size(), [&](Serializer::List &list, idx_t i) {
		auto &buffers = info.buffers[i];
		for (auto buffer : buffers) {
			list.WriteElement(buffer.buffer_ptr, buffer.allocation_size);
		}
	});
}

void WALCreateIndex::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "index_catalog_entry", index_catalog_entry);
	if (has_index_storage) {
		SerializeIndexStorage(serializer, index_storage_info);
	}
}

void WriteAheadLog::WriteCreateIndex(const IndexCatalogEntry &entry) {
	WALCreateIndex wal_entry;
	wal_entry.index_catalog_entry = entry.GetInfo();

	// Serialize the index data to the persistent storage and write the metadata.
	auto &index_entry = entry.Cast<DuckIndexEntry>();
	auto &list = index_entry.GetDataTableInfo().GetIndexes();
	wal_entry.has_index_storage =
	    GetIndexStorageInfo(GetDatabase(), list, index_entry.name, wal_entry.index_storage_info);
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropIndex(const IndexCatalogEntry &entry) {
	WALDropIndex wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// Custom Types
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateType(const TypeCatalogEntry &entry) {
	WALCreateType wal_entry;
	wal_entry.type = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropType(const TypeCatalogEntry &entry) {
	WALDropType wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// TRIGGERS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateTrigger(const TriggerCatalogEntry &entry) {
	WALCreateTrigger wal_entry;
	wal_entry.trigger = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropTrigger(const TriggerCatalogEntry &entry) {
	WALDropTrigger wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	wal_entry.table = entry.base_table->Table();
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// VIEWS
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteCreateView(const ViewCatalogEntry &entry) {
	WALCreateView wal_entry;
	wal_entry.view = entry.GetInfo();
	WriteEntry(wal_entry);
}

void WriteAheadLog::WriteDropView(const ViewCatalogEntry &entry) {
	WALDropView wal_entry;
	wal_entry.schema = entry.schema.name;
	wal_entry.name = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// DROP SCHEMA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteDropSchema(const SchemaCatalogEntry &entry) {
	WALDropSchema wal_entry;
	wal_entry.schema = entry.name;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// DATA
//===--------------------------------------------------------------------===//
void WriteAheadLog::WriteSetTable(const Identifier &schema, const Identifier &table) {
	WALUseTable wal_entry;
	wal_entry.schema = schema;
	wal_entry.table = table;
	WriteEntry(wal_entry);
}

void WALInsert::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "chunk", *write_chunk);
}

void WriteAheadLog::WriteInsert(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	chunk.Verify(GetDatabase().GetDatabase());

	WALInsert wal_entry;
	wal_entry.write_chunk = chunk;
	WriteEntry(wal_entry);
}

void WALRowGroupData::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "row_group_data", *write_data);
}

void WriteAheadLog::WriteRowGroupData(const PersistentCollectionData &data) {
	D_ASSERT(!data.row_group_data.empty());

	WALRowGroupData wal_entry;
	wal_entry.write_data = data;
	WriteEntry(wal_entry);

	// mark written blocks as checkpointed
	auto &block_manager = GetDatabase().GetStorageManager().GetBlockManager();
	for (auto &block_id : data.GetBlockIds()) {
		block_manager.MarkBlockAsCheckpointed(block_id);
	}
}

void WALDelete::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "chunk", *write_chunk);
}

void WriteAheadLog::WriteDelete(DataChunk &chunk) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 1 && chunk.data[0].GetType() == LogicalType::ROW_TYPE);
	chunk.Verify(GetDatabase().GetDatabase());

	WALDelete wal_entry;
	wal_entry.write_chunk = chunk;
	WriteEntry(wal_entry);
}

void WALUpdate::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "column_indexes", column_indexes);
	serializer.WriteProperty(102, "chunk", *write_chunk);
}

void WriteAheadLog::WriteUpdate(DataChunk &chunk, const vector<column_t> &column_indexes) {
	D_ASSERT(chunk.size() > 0);
	D_ASSERT(chunk.ColumnCount() == 2);
	D_ASSERT(chunk.data[1].GetType().id() == LogicalType::ROW_TYPE);
	chunk.Verify(GetDatabase().GetDatabase());

	WALUpdate wal_entry;
	wal_entry.column_indexes = column_indexes;
	wal_entry.write_chunk = chunk;
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// Write ALTER Statement
//===--------------------------------------------------------------------===//
void WALAlter::Serialize(Serializer &serializer) const {
	WALEntry::Serialize(serializer);
	serializer.WriteProperty(101, "info", write_info.get());
	if (has_index_storage) {
		SerializeIndexStorage(serializer, index_storage_info);
	}
}

void WriteAheadLog::WriteAlter(CatalogEntry &entry, const AlterInfo &info) {
	WALAlter wal_entry;
	wal_entry.write_info = info;

	if (info.IsAddPrimaryKey()) {
		auto &table_info = info.Cast<AlterTableInfo>();
		auto &constraint_info = table_info.Cast<AddConstraintInfo>();
		auto &unique = constraint_info.constraint->Cast<UniqueConstraint>();

		auto &table_entry = entry.Cast<DuckTableEntry>();
		auto &parent = table_entry.Parent().Cast<DuckTableEntry>();
		auto &parent_info = parent.GetStorage().GetDataTableInfo();
		auto &list = parent_info->GetIndexes();

		auto name = unique.GetName(parent.name);
		wal_entry.has_index_storage = GetIndexStorageInfo(GetDatabase(), list, name, wal_entry.index_storage_info);
	}
	WriteEntry(wal_entry);
}

//===--------------------------------------------------------------------===//
// FLUSH
//===--------------------------------------------------------------------===//
void WriteAheadLog::Flush() {
	if (!writer) {
		return;
	}

	// write an empty entry
	WALFlush wal_entry;
	WriteEntry(wal_entry);

	// flushes all changes made to the WAL to disk
	writer->Sync();
	storage_manager.SetWALSize(writer->GetFileSize());
}

void WriteAheadLog::IncrementWALEntriesCount() {
	storage_manager.IncrementWALEntriesCount();
}

} // namespace duckdb
