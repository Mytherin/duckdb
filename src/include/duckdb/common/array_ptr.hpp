//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/array_ptr.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

//! array_ptr is a non-owning pointer to an array with bounds checking enabled by default
template <class T, bool SAFE = true>
class array_ptr { // NOLINT
public:
	array_ptr(T *array_data_p, idx_t capacity_p) : array_data(array_data_p), capacity(capacity_p) {
		if (!array_data && capacity > 0) {
			throw duckdb::InternalException("array_ptr cannot be constructed with no array unless the capacity is zero");
		}
	}

	inline void BoundsCheck(idx_t index) {
		if (MemorySafety<SAFE>::enabled) {
			if (index >= capacity) {
				throw InternalException("Index %llu is out of range for an array of size %llu", index, capacity);
			}
		}
	}

	inline const T &operator[](idx_t i) const {
		BoundsCheck(i);
		return array_data[i]; // NOLINT
	}

	inline T &operator[](idx_t i) {
		BoundsCheck(i);
		return array_data[i]; // NOLINT
	}

	inline const T *get_data_unsafe() const { // NOLINT
		return array_data;
	}

	inline T *get_data_unsafe() { // NOLINT
		return array_data;
	}

	inline idx_t size() const { // NOLINT
		return capacity;
	}

	// FIXME: add iterator interface


private:
	T *array_data;
	idx_t capacity;
};

template <typename T>
using const_array_ptr = array_ptr<const T, true>;

template <typename T>
using unsafe_array_ptr = array_ptr<T, false>;

} // namespace duckdb
