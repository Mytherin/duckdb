#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/fsst_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/common/vector/shredded_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/types/vector_buffer.hpp"

#include "duckdb/common/assert.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(PhysicalType type, idx_t capacity) {
	if (type == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector requires full list type");
	}
	if (type == PhysicalType::VARCHAR) {
		return make_buffer<VectorStringBuffer>(capacity);
	}
	return make_buffer<StandardVectorBuffer>(capacity, GetTypeIdSize(type));
}

buffer_ptr<VectorBuffer> VectorBuffer::CreateStandardVector(const LogicalType &type, idx_t capacity) {
	if (type.InternalType() == PhysicalType::LIST) {
		throw InternalException("VectorBuffer::CreateStandardVector not supported for list");
	}
	return VectorBuffer::CreateStandardVector(type.InternalType(), capacity);
}

void VectorBuffer::SetSize(idx_t new_size) {
	if (!HasSize()) {
		throw InternalException("Non-Flat/Non-Constant vector buffer does not have a size defined");
	}
	if (Size() == new_size) {
		return;
	}
	throw InternalException(
	    "VectorBuffer size cannot be adjusted for this buffer type - and new size %d differs from current size %d",
	    new_size, Size());
}

idx_t VectorBuffer::GetDataSize(const LogicalType &type, idx_t count) const {
	idx_t size = 0;
	// uncompressed size of individual data entries
	size += GetTypeIdSize(type.InternalType()) * count;
	// size of validity mask
	size += GetValidityMask().GetAllocationSize();
	// size stored in aux buffers
	if (auxiliary_data) {
		for (auto &aux_data : auxiliary_data->data) {
			size += aux_data->GetAllocationSize();
		}
	}
	return size;
}

idx_t VectorBuffer::GetAllocationSize() const {
	idx_t size = 0;
	if (auxiliary_data) {
		for (auto &aux_data : auxiliary_data->data) {
			size += aux_data->GetAllocationSize();
		}
	}
	return size;
}

void VectorBuffer::Verify(const LogicalType &type, const SelectionVector &sel, idx_t count) const {
}

void VectorBuffer::SetVectorType(VectorType vector_type) {
	throw InternalException("VectorBuffer does not support SetVectorType");
}

string VectorBuffer::ToString(const LogicalType &type, idx_t count) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return GetValue(type, 0).ToString();
	}
	string retval;
	for (idx_t i = 0; i < count; i++) {
		retval += GetValue(type, i).ToString();
		if (i < count - 1) {
			retval += ", ";
		}
	}
	return retval;
}

string VectorBuffer::ToString(const LogicalType &type) const {
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		return GetValue(type, 0).ToString();
	}
	return "";
}

buffer_ptr<VectorBuffer> VectorBuffer::Resize(const LogicalType &type, idx_t current_size, idx_t new_size) {
	throw InternalException("VectorBuffer::Resize not supported for this vector type");
}

void VectorBuffer::ToUnifiedFormat(idx_t count, UnifiedVectorFormat &format) const {
	throw InternalException("ToUnifiedFormat not supported for this buffer type - flatten first");
}

buffer_ptr<VectorBuffer> VectorBuffer::Flatten(const LogicalType &type, idx_t count) const {
	// FIXME: this should just be using size.GetIndex()...
	if (size.IsValid()) {
		if (count > size.GetIndex()) {
			throw InternalException("Flatten called with count that exceeds the size of the vector");
		}
		count = size.GetIndex();
	}
	auto result = FlattenSliceInternal(type, *FlatVector::IncrementalSelectionVector(), count);
	if (result && (!result->HasSize() || result->Size() != count)) {
		throw InternalException("FlattenSliceInternal did not set size correctly");
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::FlattenSlice(const LogicalType &type, const SelectionVector &sel,
                                                    idx_t count) const {
	buffer_ptr<VectorBuffer> result;
	if (vector_type == VectorType::CONSTANT_VECTOR) {
		// if the vector is a constant vector the input selection vector does not matter
		// we always need to select the first value
		SelectionVector owned_sel;
		auto &constant_sel = *ConstantVector::ZeroSelectionVector(count, owned_sel);
		result = FlattenSliceInternal(type, constant_sel, count);
	} else {
		result = FlattenSliceInternal(type, sel, count);
	}
	if (result && (!result->HasSize() || result->Size() != count)) {
		throw InternalException("FlattenSliceInternal did not set size correctly");
	}
	return result;
}

buffer_ptr<VectorBuffer> VectorBuffer::FlattenSliceInternal(const LogicalType &type, const SelectionVector &sel,
                                                            idx_t count) const {
	throw InternalException("Unimplemented type for flatten");
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, idx_t offset, idx_t end) {
	if (vector_type == VectorType::CONSTANT_VECTOR && HasSize() && end - offset == Size()) {
		return nullptr;
	}
	return SliceInternal(type, offset, end);
}

buffer_ptr<VectorBuffer> VectorBuffer::Slice(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	if (vector_type == VectorType::CONSTANT_VECTOR && HasSize() && count == Size()) {
		return nullptr;
	}
	return SliceInternal(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceWithCache(SelCache &cache, const LogicalType &type,
                                                      const SelectionVector &sel, idx_t count) {
	return Slice(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceInternal(const LogicalType &type, idx_t offset, idx_t end) {
	// we can slice the data directly only for standard vectors
	// for non-flat vectors slice using a selection vector instead
	idx_t count = end - offset;
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, offset + i);
	}
	return Slice(type, sel, count);
}

buffer_ptr<VectorBuffer> VectorBuffer::SliceInternal(const LogicalType &type, const SelectionVector &sel, idx_t count) {
	// default slice: flatten with a selection vector and then wrap in a dictionary
	return FlattenSlice(type, sel, count);
}

void VectorBuffer::SetValue(const LogicalType &type, idx_t index, const Value &val) {
	if (index >= Capacity()) {
		throw InternalException("VectorBuffer::SetValue out of range for vector capacity - attempting to set index %d for vector with capacity %d", index, Capacity());
	}
	idx_t max_index;
	if (!HasSize()) {
		max_index = 0;
	} else {
		max_index = Size();
	}
	if (index > max_index) {
		throw InternalException("VectorBuffer::SetValue non-consecutive for vector - attempting to set index %d for vector, but current size is %d\nCannot grow vector to fit this value without creating uninitialized spaces", index, max_index);
	}
	SetValueInternal(type, index, val);
	if (index == max_index) {
		// grow the size
		SetSize(max_index + 1);
	}
}

Value VectorBuffer::GetValue(const LogicalType &type, idx_t index) const {
	if (vector_type != VectorType::CONSTANT_VECTOR && HasSize() && index >= Size()) {
		throw InternalException("VectorBuffer::GetValue out of range for vector - attempting to access index %d for vector of size %d", index, Size());
	}
	return GetValueInternal(type, index);
}

Value VectorBuffer::GetValueInternal(const LogicalType &type, idx_t index) const {
	throw InternalException("SetValue not supported for this buffer type");
}

void VectorBuffer::SetValueInternal(const LogicalType &type, idx_t index, const Value &val) {
	throw InternalException("Unimplemented GetValue for this buffer type");
}


PinnedBufferHolder::PinnedBufferHolder(BufferHandle handle) : handle(std::move(handle)) {
}

PinnedBufferHolder::~PinnedBufferHolder() {
}

} // namespace duckdb
