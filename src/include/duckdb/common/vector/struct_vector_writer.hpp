//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/struct_vector_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

namespace duckdb {

//! VectorWriter specialization for FlatStruct - writes into struct vectors
template <class... Args>
struct VectorWriter<FlatStruct<Args...>> {
	VectorWriter(Vector &vector, idx_t count)
	    : validity(FlatVector::Validity(vector)), count(count),
	      children(StructVector::GetEntries(vector), count, std::index_sequence_for<Args...> {}) {
	}

	void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
		children.SetInvalid(idx);
	}

private:
	ValidityMask &validity;
	idx_t count;

public:
	StructWriterChildren<Args...> children;
};

} // namespace duckdb
