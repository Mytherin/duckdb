#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/assert.hpp"

namespace duckdb {

struct StorageLockInternals : enable_shared_from_this<StorageLockInternals> {
public:
	std::shared_mutex rw_lock;

public:
	unique_ptr<StorageLockKey> GetExclusiveLock() {
		std::unique_lock<std::shared_mutex> lk(rw_lock);
		return make_uniq<StorageLockKey>(shared_from_this(), std::move(lk));
	}

	unique_ptr<StorageLockKey> GetSharedLock() {
		std::shared_lock<std::shared_mutex> lk(rw_lock);
		return make_uniq<StorageLockKey>(shared_from_this(), std::move(lk));
	}

	unique_ptr<StorageLockKey> TryGetExclusiveLock() {
		std::unique_lock<std::shared_mutex> lk(rw_lock, std::try_to_lock);
		if (!lk.owns_lock()) {
			return nullptr;
		}
		return make_uniq<StorageLockKey>(shared_from_this(), std::move(lk));
	}

	unique_ptr<StorageLockKey> TryUpgradeCheckpointLock(StorageLockKey &lock)
	    __attribute__((no_thread_safety_analysis)) {
		if (lock.GetType() != StorageLockType::SHARED) {
			throw InternalException("StorageLock::TryUpgradeLock called on an exclusive lock");
		}
		// Release the shared lock before attempting exclusive to avoid undefined behavior
		auto &slock = std::get<std::shared_lock<std::shared_mutex>>(lock.lock_holder);
		slock.unlock();
		std::unique_lock<std::shared_mutex> ulock(rw_lock, std::try_to_lock);
		if (!ulock.owns_lock()) {
			// Another writer or reader got in — re-acquire shared and report failure
			slock.lock();
			return nullptr;
		}
		// Exclusive acquired. The original shared key's lock_holder now holds a non-owning
		// shared_lock (owns_lock() == false), so its destructor will not call unlock_shared().
		return make_uniq<StorageLockKey>(shared_from_this(), std::move(ulock));
	}
};

StorageLockKey::StorageLockKey(shared_ptr<StorageLockInternals> internals_p, std::unique_lock<std::shared_mutex> lock)
    : internals(std::move(internals_p)), lock_holder(std::move(lock)), type(StorageLockType::EXCLUSIVE) {
}

StorageLockKey::StorageLockKey(shared_ptr<StorageLockInternals> internals_p, std::shared_lock<std::shared_mutex> lock)
    : internals(std::move(internals_p)), lock_holder(std::move(lock)), type(StorageLockType::SHARED) {
}

StorageLock::StorageLock() : internals(make_shared_ptr<StorageLockInternals>()) {
}
StorageLock::~StorageLock() {
}

unique_ptr<StorageLockKey> StorageLock::GetExclusiveLock() {
	return internals->GetExclusiveLock();
}

unique_ptr<StorageLockKey> StorageLock::TryGetExclusiveLock() {
	return internals->TryGetExclusiveLock();
}

unique_ptr<StorageLockKey> StorageLock::GetSharedLock() {
	return internals->GetSharedLock();
}

unique_ptr<StorageLockKey> StorageLock::TryUpgradeCheckpointLock(StorageLockKey &lock) {
	return internals->TryUpgradeCheckpointLock(lock);
}

} // namespace duckdb
