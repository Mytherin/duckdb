//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/type_util.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/uhugeint.hpp"
#include "duckdb/common/types/double_na_equal.hpp"

namespace duckdb {
struct varint_t;

//! Returns the PhysicalType for the given type
template <class T>
PhysicalType GetTypeId() {
	if (std::is_same<T, bool>()) {
		return PhysicalType::BOOL;
	} else if (std::is_same<T, int8_t>()) {
		return PhysicalType::INT8;
	} else if (std::is_same<T, int16_t>()) {
		return PhysicalType::INT16;
	} else if (std::is_same<T, int32_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<T, int64_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, uint8_t>()) {
		return PhysicalType::UINT8;
	} else if (std::is_same<T, uint16_t>()) {
		return PhysicalType::UINT16;
	} else if (std::is_same<T, uint32_t>()) {
		return PhysicalType::UINT32;
	} else if (std::is_same<T, uint64_t>()) {
		return PhysicalType::UINT64;
	} else if (std::is_same<T, idx_t>() || std::is_same<T, const idx_t>()) {
		return PhysicalType::UINT64;
	} else if (std::is_same<T, hugeint_t>()) {
		return PhysicalType::INT128;
	} else if (std::is_same<T, uhugeint_t>()) {
		return PhysicalType::UINT128;
	} else if (std::is_same<T, date_t>()) {
		return PhysicalType::INT32;
	} else if (std::is_same<T, dtime_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, dtime_tz_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, dtime_ns_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, timestamp_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, timestamp_sec_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, timestamp_ms_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, timestamp_ns_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, timestamp_tz_t>()) {
		return PhysicalType::INT64;
	} else if (std::is_same<T, float>() || std::is_same<T, float_na_equal>()) {
		return PhysicalType::FLOAT;
	} else if (std::is_same<T, double>() || std::is_same<T, double_na_equal>()) {
		return PhysicalType::DOUBLE;
	} else if (std::is_same<T, const char *>() || std::is_same<T, char *>() || std::is_same<T, string_t>() ||
	           std::is_same<T, varint_t>()) {
		return PhysicalType::VARCHAR;
	} else if (std::is_same<T, interval_t>()) {
		return PhysicalType::INTERVAL;
	} else if (std::is_same<T, list_entry_t>()) {
		return PhysicalType::LIST;
	} else if (std::is_pointer<T>() || std::is_same<T, uintptr_t>()) {
		if (sizeof(uintptr_t) == sizeof(uint32_t)) {
			return PhysicalType::UINT32;
		} else if (sizeof(uintptr_t) == sizeof(uint64_t)) {
			return PhysicalType::UINT64;
		} else {
			throw InternalException("Unsupported pointer size in GetTypeId");
		}
	} else {
		throw InternalException("Unsupported type in GetTypeId");
	}
}

template <class T>
bool StorageTypeCompatible(PhysicalType type) {
	if (std::is_same<T, int8_t>()) {
		return type == PhysicalType::INT8 || type == PhysicalType::BOOL;
	}
	if (std::is_same<T, uint8_t>()) {
		return type == PhysicalType::UINT8 || type == PhysicalType::BOOL;
	}
	return type == GetTypeId<T>();
}

template <class T>
bool TypeIsNumber() {
	return std::is_integral<T>() || std::is_floating_point<T>() || std::is_same<T, hugeint_t>() ||
	       std::is_same<T, uhugeint_t>();
}

template <class T>
bool IsValidType() {
	return GetTypeId<T>() != PhysicalType::INVALID;
}

template <class T>
bool IsIntegerType() {
	return TypeIsIntegral(GetTypeId<T>());
}

} // namespace duckdb
