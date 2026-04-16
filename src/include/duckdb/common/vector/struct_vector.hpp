//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/struct_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

class VectorStructBuffer : public VectorBuffer {
public:
	explicit VectorStructBuffer(const LogicalType &struct_type, idx_t capacity = STANDARD_VECTOR_SIZE);
	VectorStructBuffer(vector<Vector> children, idx_t capacity);
	VectorStructBuffer(VectorStructBuffer &other, const SelectionVector &sel, idx_t count);
	~VectorStructBuffer() override;

public:
	ValidityMask &GetValidityMask() override {
		return validity;
	}
	idx_t Capacity() const override {
		return capacity;
	}
	void SetCapacity(idx_t new_capacity) {
		capacity = new_capacity;
	}
	void ResetCache(idx_t capacity) override;
	void SetSize(idx_t new_size) override;
	const ValidityMask &GetValidityMask() const override {
		return validity;
	}
	const vector<Vector> &GetChildren() const {
		return children;
	}
	vector<Vector> &GetChildren() {
		return children;
	}
	void SetVectorType(VectorType vector_type) override;

public:
	idx_t GetDataSize(const LogicalType &type, idx_t count) const override;
	idx_t GetAllocationSize() const override;
	void ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const override;
	buffer_ptr<VectorBuffer> Flatten(const LogicalType &type, idx_t count) const override;
	void Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const override;
	buffer_ptr<VectorBuffer> Resize(const LogicalType &type, idx_t current_size, idx_t new_size) override;

protected:
	Value GetValueInternal(const LogicalType &type, idx_t index) const override;
	void SetValueInternal(const LogicalType &type, idx_t index, const Value &val) override;
	buffer_ptr<VectorBuffer> FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
	                                              idx_t count) const override;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, idx_t offset, idx_t end) override;
	buffer_ptr<VectorBuffer> SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) override;

private:
	ValidityMask validity;
	//! child vectors used for nested data
	vector<Vector> children;
	//! The capacity of the struct
	idx_t capacity;
};

struct StructVector {
	DUCKDB_API static const vector<Vector> &GetEntries(const Vector &vector);
	DUCKDB_API static vector<Vector> &GetEntries(Vector &vector);
};

} // namespace duckdb
