//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/storage_lock.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/shared_ptr.hpp"

#include <shared_mutex>
#include <variant>

namespace duckdb {
struct StorageLockInternals;

enum class StorageLockType { SHARED = 0, EXCLUSIVE = 1 };

class StorageLockKey {
	friend struct StorageLockInternals;

public:
	StorageLockKey(shared_ptr<StorageLockInternals> internals, std::unique_lock<std::shared_mutex> lock);
	StorageLockKey(shared_ptr<StorageLockInternals> internals, std::shared_lock<std::shared_mutex> lock);
	~StorageLockKey() = default;

	StorageLockType GetType() const {
		return type;
	}

private:
	// internals must be declared before lock_holder: members are destroyed in reverse
	// declaration order, so the lock is released before the shared_mutex is potentially destroyed.
	shared_ptr<StorageLockInternals> internals;
	std::variant<std::unique_lock<std::shared_mutex>, std::shared_lock<std::shared_mutex>> lock_holder;
	StorageLockType type;
};

class StorageLock {
public:
	StorageLock();
	~StorageLock();

	//! Get an exclusive lock
	unique_ptr<StorageLockKey> GetExclusiveLock();
	//! Get a shared lock
	unique_ptr<StorageLockKey> GetSharedLock();
	//! Try to get an exclusive lock - if we cannot get it immediately we return `nullptr`
	unique_ptr<StorageLockKey> TryGetExclusiveLock();
	//! This is a special method that only exists for checkpointing
	//! This method takes a shared lock, and returns an exclusive lock if the parameter is the only active shared lock
	//! If this method succeeds, the original shared lock is released and the returned key is exclusive
	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock);

private:
	shared_ptr<StorageLockInternals> internals;
};

} // namespace duckdb
