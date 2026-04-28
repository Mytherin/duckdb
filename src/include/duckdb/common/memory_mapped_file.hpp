//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/memory_mapped_file.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/file_open_flags.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! Options for opening a memory-mapped file via FileSystem::MemoryMapFile.
struct MMapOptions {
	//! Size of the virtual mapping. For writable mappings the underlying file is sparsely
	//! extended to at least this size up front so that writes anywhere within the range
	//! are visible through the mapping without ever needing to remap. Read-only mappings
	//! always cover exactly the existing file size and ignore this field.
	idx_t reserve_size = 0;
};

//! A memory-mapped view of a file. Callers obtain a pointer into the mapped region via
//! GetData / GetDataMutable and operate on it directly (typically by memcpy). Sync flushes
//! pending writes to disk.
//!
//! The mapping is created at a fixed size at construction time (the "reserve size") and
//! that size never changes for the lifetime of the object. For writable mappings, the
//! underlying file is extended (sparsely) to the reserve size up front, so writes anywhere
//! within the reserve range are visible through the pointer without any remapping.
//!
//! Pointers returned by GetData / GetDataMutable are stable for the lifetime of the
//! MemoryMappedFile and may be used concurrently by multiple threads — no internal
//! synchronization is necessary because the mapping is never moved.
class MemoryMappedFile {
public:
	DUCKDB_API MemoryMappedFile(string path, FileOpenFlags flags);
	MemoryMappedFile(const MemoryMappedFile &) = delete;
	DUCKDB_API virtual ~MemoryMappedFile();

	//! Return a const pointer at [location] in the mapped region. Bounds-checked: throws
	//! if [location, location + nr_bytes) is not fully covered by the current mapping.
	DUCKDB_API const_data_ptr_t GetData(idx_t location, idx_t nr_bytes) const;
	//! Return a mutable pointer at [location] in the mapped region. Bounds-checked. The
	//! mapping must have been opened with WRITE access.
	DUCKDB_API data_ptr_t GetDataMutable(idx_t location, idx_t nr_bytes);

	//! Flush all pending writes to disk (msync / FlushViewOfFile + FlushFileBuffers).
	DUCKDB_API virtual void Sync() = 0;
	//! Punch a hole in the underlying file at [offset, offset + length), releasing the
	//! backing disk space without changing the file size or invalidating the mapping.
	//! Returns true if the operation was performed; false if the platform does not support it.
	DUCKDB_API virtual bool Trim(idx_t offset, idx_t length) = 0;
	//! Whether the underlying storage is a regular on-disk file (as opposed to e.g. a remote
	//! object). Memory-mapped files are always backed by a local on-disk file.
	DUCKDB_API virtual bool OnDiskFile() const {
		return true;
	}
	//! Close the mapping and the underlying file. Invalidates any previously-returned pointer.
	DUCKDB_API virtual void Close() = 0;

	//! Total size of the mapped region (the reserve size). The underlying file is at least
	//! this large for writable mappings.
	DUCKDB_API idx_t Size() const {
		return size;
	}
	const string &GetPath() const {
		return path;
	}
	FileOpenFlags GetFlags() const {
		return flags;
	}

public:
	string path;
	FileOpenFlags flags;

protected:
	data_ptr_t data = nullptr;
	idx_t size = 0;
};

} // namespace duckdb
