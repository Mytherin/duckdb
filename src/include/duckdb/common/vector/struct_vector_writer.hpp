//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/struct_vector_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/vector_writer.hpp"

namespace duckdb {

//! VectorWriter specialization for FixedStruct - writes into struct vectors
template <class... Args>
struct VectorWriter<FixedStruct<Args...>> {
	static constexpr bool IS_HOMOGENEOUS = AllSameType<Args...>::value;
	using ChildrenType = StructWriterChildren<IS_HOMOGENEOUS, Args...>;

	VectorWriter(Vector &vector, idx_t count)
	    : validity(FlatVector::Validity(vector)), count(count),
	      children(StructVector::GetEntries(vector), count, std::index_sequence_for<Args...> {}) {
	}

	void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
		children.SetInvalid(idx);
	}

	ChildrenType children;

private:
	ValidityMask &validity;
	idx_t count;
};

} // namespace duckdb
