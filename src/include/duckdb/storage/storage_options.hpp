//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_options.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/encryption_state.hpp"

namespace duckdb {

enum class CompressInMemory { AUTOMATIC, COMPRESS, DO_NOT_COMPRESS };

//! How to perform I/O against the database file.
enum class FileIOMode : uint8_t {
	//! Use buffered read()/write() (or pread/pwrite) syscalls through a FileHandle.
	BUFFERED_IO,
	//! Memory-map the file and route reads through the mapped region.
	MMAP,
};

struct StorageOptions {
	//! The allocation size of blocks for this attached database file (if any)
	optional_idx block_alloc_size;
	//! The row group size for this attached database (if any)
	optional_idx row_group_size;
	//! Target storage version (if any)
	optional_idx storage_version;
	//! Block header size (only used for encryption)
	optional_idx block_header_size;

	CompressInMemory compress_in_memory = CompressInMemory::AUTOMATIC;

	//! How to perform I/O against the database file (buffered syscalls vs memory-mapped reads).
	//! Empty when no IO_MODE was specified at attach time; the StorageManager fills in the
	//! per-database value from the `default_io_mode` setting in that case.
	optional<FileIOMode> io_mode;

	//! Whether the database is encrypted
	bool encryption = false;
	//! Encryption algorithm
	EncryptionTypes::CipherType encryption_cipher = EncryptionTypes::INVALID;
	//! encryption key
	//! FIXME: change to a unique_ptr in the future
	shared_ptr<string> user_key;
	//! encryption version (set default to 1)
	EncryptionTypes::EncryptionVersion encryption_version = EncryptionTypes::NONE;

	void SetEncryptionVersion(string &storage_version_user_provided);
	void Initialize(unordered_map<string, Value> &options);
};

} // namespace duckdb
