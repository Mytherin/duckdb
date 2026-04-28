#include "duckdb/common/memory_mapped_file.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/numeric_utils.hpp"

#ifdef _WIN32
#include "duckdb/common/windows.hpp"
#include "duckdb/common/windows_util.hpp"
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace duckdb {

//===--------------------------------------------------------------------===//
// MemoryMappedFile (base class)
//===--------------------------------------------------------------------===//
MemoryMappedFile::MemoryMappedFile(string path_p, FileOpenFlags flags_p) : path(std::move(path_p)), flags(flags_p) {
}

MemoryMappedFile::~MemoryMappedFile() {
}

const_data_ptr_t MemoryMappedFile::GetData(idx_t location, idx_t nr_bytes) const {
	if (location + nr_bytes > size) {
		throw IOException("MemoryMappedFile::GetData: out-of-bounds access in file \"%s\" "
		                  "(location=%llu, nr_bytes=%llu, size=%llu)",
		                  path, location, nr_bytes, size);
	}
	return data + location;
}

data_ptr_t MemoryMappedFile::GetDataMutable(idx_t location, idx_t nr_bytes) {
	if (!flags.OpenForWriting()) {
		throw IOException("MemoryMappedFile::GetDataMutable: file \"%s\" was not opened for writing", path);
	}
	if (location + nr_bytes > size) {
		throw IOException("MemoryMappedFile::GetDataMutable: out-of-bounds access in file \"%s\" "
		                  "(location=%llu, nr_bytes=%llu, size=%llu)",
		                  path, location, nr_bytes, size);
	}
	return data + location;
}

#ifndef _WIN32
//===--------------------------------------------------------------------===//
// Unix implementation (Linux, macOS, *BSD)
//===--------------------------------------------------------------------===//
class UnixMemoryMappedFile : public MemoryMappedFile {
public:
	UnixMemoryMappedFile(string path_p, FileOpenFlags flags_p, int fd_p, idx_t initial_file_size, idx_t reserve_size)
	    : MemoryMappedFile(std::move(path_p), flags_p), fd(fd_p) {
		// For writable mappings, sparsely extend the file to the reserve size so that the
		// mmap region covers it without ever needing to be remapped. For read-only mappings
		// we cannot grow the file, so the mapping spans the existing file size.
		if (flags.OpenForWriting()) {
			size = MaxValue<idx_t>(initial_file_size, reserve_size);
			if (initial_file_size < size) {
				if (ftruncate(fd, NumericCast<off_t>(size)) != 0) {
					int saved_errno = errno;
					throw IOException({{"errno", std::to_string(saved_errno)}},
					                  "Could not ftruncate file \"%s\" to reserve size %llu: %s", path, size,
					                  strerror(saved_errno));
				}
			}
		} else {
			size = initial_file_size;
		}
		if (size > 0) {
			int prot = PROT_READ;
			if (flags.OpenForWriting()) {
				prot |= PROT_WRITE;
			}
			void *mapped = mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
			if (mapped == MAP_FAILED) {
				int saved_errno = errno;
				throw IOException({{"errno", std::to_string(saved_errno)}},
				                  "Could not mmap file \"%s\" (size=%llu): %s", path, size, strerror(saved_errno));
			}
			data = data_ptr_cast(mapped);
		}
	}

	~UnixMemoryMappedFile() override {
		try {
			UnixMemoryMappedFile::Close();
		} catch (...) {
			// not allowed to throw in destructor
		}
	}

	void Sync() override {
		if (data == nullptr || size == 0) {
			return;
		}
		if (msync(data, size, MS_SYNC) != 0) {
			throw IOException({{"errno", std::to_string(errno)}}, "Could not msync file \"%s\": %s", path,
			                  strerror(errno));
		}
	}

	bool Trim(idx_t offset, idx_t length) override {
#if defined(__linux__) && !(defined(__GLIBC__) && (__GLIBC__ < 2 || (__GLIBC__ == 2 && __GLIBC_MINOR__ < 18)))
		int rc = fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, NumericCast<off_t>(offset),
		                   NumericCast<off_t>(length));
		return rc == 0;
#else
		(void)offset;
		(void)length;
		return false;
#endif
	}

	void Close() override {
		if (data != nullptr) {
			munmap(data, size);
			data = nullptr;
			size = 0;
		}
		if (fd != -1) {
			close(fd);
			fd = -1;
		}
	}

private:
	int fd;
};

unique_ptr<MemoryMappedFile> LocalFileSystem::MemoryMapFile(const OpenFileInfo &path_info, FileOpenFlags flags,
                                                            const MMapOptions &options,
                                                            optional_ptr<FileOpener> opener) {
	auto path = ExpandPath(path_info.path, opener);
	flags.Verify();

	bool open_read = flags.OpenForReading();
	bool open_write = flags.OpenForWriting();
	int open_flags;
	if (open_read && open_write) {
		open_flags = O_RDWR;
	} else if (open_read) {
		open_flags = O_RDONLY;
	} else {
		throw InternalException("READ or READ+WRITE must be specified when memory-mapping a file");
	}
	if (open_write && flags.CreateFileIfNotExists()) {
		open_flags |= O_CREAT;
	}

	int fd = open(path.c_str(), open_flags, 0666);
	if (fd == -1) {
		if (flags.ReturnNullIfNotExists() && errno == ENOENT) {
			return nullptr;
		}
		throw IOException({{"errno", std::to_string(errno)}}, "Cannot open file \"%s\" for memory mapping: %s", path,
		                  strerror(errno));
	}

	// Take a POSIX advisory lock per flags.Lock() to prevent another DuckDB process from
	// mapping the same file concurrently.
	TryAcquireFileLock(*this, fd, path, flags);

	struct stat st;
	if (fstat(fd, &st) != 0) {
		int saved_errno = errno;
		close(fd);
		throw IOException({{"errno", std::to_string(saved_errno)}}, "Could not stat file \"%s\": %s", path,
		                  strerror(saved_errno));
	}
	idx_t initial_file_size = NumericCast<idx_t>(st.st_size);

	return make_uniq<UnixMemoryMappedFile>(path, flags, fd, initial_file_size, options.reserve_size);
}

#else
//===--------------------------------------------------------------------===//
// Windows implementation
//===--------------------------------------------------------------------===//
class WindowsMemoryMappedFile : public MemoryMappedFile {
public:
	WindowsMemoryMappedFile(string path_p, FileOpenFlags flags_p, HANDLE file_handle_p, idx_t initial_file_size,
	                        idx_t reserve_size)
	    : MemoryMappedFile(std::move(path_p), flags_p), file_handle(file_handle_p), mapping_handle(nullptr) {
		if (flags.OpenForWriting()) {
			size = MaxValue<idx_t>(initial_file_size, reserve_size);
			if (initial_file_size < size) {
				// Mark the file as sparse so the extension below doesn't actually allocate disk space
				// for the un-touched range.
				DWORD bytes_returned = 0;
				DeviceIoControl(file_handle, FSCTL_SET_SPARSE, NULL, 0, NULL, 0, &bytes_returned, NULL);
				LARGE_INTEGER li;
				li.QuadPart = static_cast<LONGLONG>(size);
				if (!SetFilePointerEx(file_handle, li, NULL, FILE_BEGIN)) {
					throw IOException("Could not SetFilePointerEx for \"%s\": %s", path,
					                  LocalFileSystem::GetLastErrorAsString());
				}
				if (!SetEndOfFile(file_handle)) {
					throw IOException("Could not SetEndOfFile for \"%s\" (reserve_size=%llu): %s", path, size,
					                  LocalFileSystem::GetLastErrorAsString());
				}
			}
		} else {
			size = initial_file_size;
		}
		if (size > 0) {
			DWORD protect = flags.OpenForWriting() ? PAGE_READWRITE : PAGE_READONLY;
			DWORD desired_access = flags.OpenForWriting() ? FILE_MAP_WRITE : FILE_MAP_READ;
			DWORD high = static_cast<DWORD>((static_cast<uint64_t>(size) >> 32) & 0xFFFFFFFFULL);
			DWORD low = static_cast<DWORD>(static_cast<uint64_t>(size) & 0xFFFFFFFFULL);
			mapping_handle = CreateFileMappingW(file_handle, NULL, protect, high, low, NULL);
			if (mapping_handle == NULL) {
				throw IOException("Could not CreateFileMapping for \"%s\": %s", path,
				                  LocalFileSystem::GetLastErrorAsString());
			}
			void *mapped = MapViewOfFile(mapping_handle, desired_access, 0, 0, size);
			if (mapped == NULL) {
				auto err = LocalFileSystem::GetLastErrorAsString();
				CloseHandle(mapping_handle);
				mapping_handle = nullptr;
				throw IOException("Could not MapViewOfFile for \"%s\": %s", path, err);
			}
			data = data_ptr_cast(mapped);
		}
	}

	~WindowsMemoryMappedFile() override {
		try {
			WindowsMemoryMappedFile::Close();
		} catch (...) {
			// not allowed to throw in destructor
		}
	}

	void Sync() override {
		if (data == nullptr || size == 0) {
			return;
		}
		if (!FlushViewOfFile(data, size)) {
			throw IOException("Could not FlushViewOfFile for \"%s\": %s", path,
			                  LocalFileSystem::GetLastErrorAsString());
		}
		if (!FlushFileBuffers(file_handle)) {
			throw IOException("Could not FlushFileBuffers for \"%s\": %s", path,
			                  LocalFileSystem::GetLastErrorAsString());
		}
	}

	bool Trim(idx_t offset, idx_t length) override {
		// Hole-punching via FSCTL_SET_ZERO_DATA is supported on NTFS sparse files but is not
		// implemented here yet. The Unix LocalFileSystem::Trim path returns false in this
		// situation, so we mirror that.
		(void)offset;
		(void)length;
		return false;
	}

	void Close() override {
		if (data != nullptr) {
			UnmapViewOfFile(data);
			data = nullptr;
			size = 0;
		}
		if (mapping_handle != nullptr) {
			CloseHandle(mapping_handle);
			mapping_handle = nullptr;
		}
		if (file_handle != nullptr && file_handle != INVALID_HANDLE_VALUE) {
			CloseHandle(file_handle);
			file_handle = nullptr;
		}
	}

private:
	HANDLE file_handle;
	HANDLE mapping_handle;
};

unique_ptr<MemoryMappedFile> LocalFileSystem::MemoryMapFile(const OpenFileInfo &path_info, FileOpenFlags flags,
                                                            const MMapOptions &options,
                                                            optional_ptr<FileOpener> opener) {
	auto path = ExpandPath(path_info.path, opener);
	flags.Verify();

	bool open_read = flags.OpenForReading();
	bool open_write = flags.OpenForWriting();
	DWORD desired_access;
	DWORD share_mode;
	DWORD creation_disposition = OPEN_EXISTING;

	if (open_read && open_write) {
		desired_access = GENERIC_READ | GENERIC_WRITE;
	} else if (open_read) {
		desired_access = GENERIC_READ;
	} else {
		throw InternalException("READ or READ+WRITE must be specified when memory-mapping a file");
	}
	// Mirror LocalFileSystem::OpenFile's share_mode handling so that a write lock prevents
	// concurrent opens and a read lock allows concurrent readers but blocks writers.
	switch (flags.Lock()) {
	case FileLockType::NO_LOCK:
		share_mode = FILE_SHARE_READ | FILE_SHARE_WRITE;
		break;
	case FileLockType::READ_LOCK:
		share_mode = FILE_SHARE_READ;
		break;
	case FileLockType::WRITE_LOCK:
		share_mode = 0;
		break;
	default:
		throw InternalException("Unknown FileLockType");
	}
	share_mode |= FILE_SHARE_DELETE;
	if (open_write && flags.CreateFileIfNotExists()) {
		creation_disposition = OPEN_ALWAYS;
	}

	auto unicode_path = WindowsUtil::UTF8ToUnicode(path.c_str());
	HANDLE file_handle = CreateFileW(unicode_path.c_str(), desired_access, share_mode, NULL, creation_disposition,
	                                 FILE_ATTRIBUTE_NORMAL, NULL);
	if (file_handle == INVALID_HANDLE_VALUE) {
		if (flags.ReturnNullIfNotExists() && GetLastError() == ERROR_FILE_NOT_FOUND) {
			return nullptr;
		}
		throw IOException("Cannot open file \"%s\" for memory mapping: %s", path,
		                  LocalFileSystem::GetLastErrorAsString());
	}

	LARGE_INTEGER size_li;
	if (!GetFileSizeEx(file_handle, &size_li)) {
		auto err = LocalFileSystem::GetLastErrorAsString();
		CloseHandle(file_handle);
		throw IOException("Could not GetFileSizeEx for \"%s\": %s", path, err);
	}
	idx_t initial_file_size = NumericCast<idx_t>(size_li.QuadPart);

	return make_uniq<WindowsMemoryMappedFile>(path, flags, file_handle, initial_file_size, options.reserve_size);
}
#endif

//===--------------------------------------------------------------------===//
// FileSystem default implementation
//===--------------------------------------------------------------------===//
unique_ptr<MemoryMappedFile> FileSystem::MemoryMapFile(const OpenFileInfo &path, FileOpenFlags flags,
                                                       const MMapOptions &options, optional_ptr<FileOpener> opener) {
	throw NotImplementedException("Memory-mapped file access is not supported by this file system (%s) for path \"%s\"",
	                              GetName(), path.path);
}

} // namespace duckdb
