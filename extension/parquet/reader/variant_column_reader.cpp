#include <stdint.h>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>

#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "reader/variant_column_reader.hpp"
#include "reader/variant/variant_shredded_conversion.hpp"
#include "column_reader.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/variant_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "parquet_column_schema.hpp"

namespace duckdb_apache {
namespace thrift {
namespace protocol {
class TProtocol;
} // namespace protocol
} // namespace thrift
} // namespace duckdb_apache
namespace duckdb_parquet {
class ColumnChunk;
} // namespace duckdb_parquet

namespace duckdb {
class ClientContext;
class ParquetReader;
class ThriftFileTransport;

//===--------------------------------------------------------------------===//
// Variant Column Reader
//===--------------------------------------------------------------------===//
VariantColumnReader::VariantColumnReader(ClientContext &context, const ParquetReader &reader,
                                         const ParquetColumnSchema &schema,
                                         vector<unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, schema), context(context), child_readers(std::move(child_readers_p)) {
	D_ASSERT(Type().InternalType() == PhysicalType::STRUCT);

	if (child_readers[0]->Schema().name == "metadata" && child_readers[1]->Schema().name == "value") {
		metadata_reader_idx = 0;
		value_reader_idx = 1;
	} else if (child_readers[1]->Schema().name == "metadata" && child_readers[0]->Schema().name == "value") {
		metadata_reader_idx = 1;
		value_reader_idx = 0;
	} else {
		throw InternalException("The Variant column must have 'metadata' and 'value' as the first two columns");
	}
}

ColumnReader &VariantColumnReader::GetChildReader(idx_t child_idx) {
	if (!child_readers[child_idx]) {
		throw InternalException("VariantColumnReader::GetChildReader(%d) - but this child reader is not set",
		                        child_idx);
	}
	return *child_readers[child_idx].get();
}

void VariantColumnReader::InitializeRead(idx_t row_group_idx_p, const vector<ColumnChunk> &columns,
                                         TProtocol &protocol_p) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->InitializeRead(row_group_idx_p, columns, protocol_p);
	}
}

static LogicalType GetIntermediateGroupType(optional_ptr<ColumnReader> typed_value) {
	child_list_t<LogicalType> children;
	children.emplace_back("value", LogicalType::BLOB);
	if (typed_value) {
		children.emplace_back("typed_value", typed_value->Type());
	}
	return LogicalType::STRUCT(std::move(children));
}

//===--------------------------------------------------------------------===//
// Direct shredded emission (fast path)
//===--------------------------------------------------------------------===//
// When a chunk is "fully shredded" - i.e. every Parquet 'value' overlay blob (at every level of the
// group tree) is NULL - we construct DuckDB's native SHREDDED_VECTOR representation directly, instead
// of decoding every value into a vector<VariantValue> tree and re-encoding it. The unshredded (overlay)
// component is a CONSTANT NULL vector, so nothing ever references it.

//! Returns the named child ('value' / 'typed_value') of a Parquet variant group, or nullptr if absent
static optional_ptr<Vector> TryGetGroupChild(Vector &group, const string &name) {
	auto &entries = StructVector::GetEntries(group);
	auto &children = StructType::GetChildTypes(group.GetType());
	for (idx_t i = 0; i < children.size(); i++) {
		if (children[i].first == name) {
			return &entries[i];
		}
	}
	return nullptr;
}

static Vector &GetGroupChild(Vector &group, const string &name) {
	auto child = TryGetGroupChild(group, name);
	if (!child) {
		throw InternalException("Variant group does not contain a '%s' column", name);
	}
	return *child;
}

//! Whether every 'value' overlay blob at every level of the group is NULL for all 'count' rows
static bool GroupIsFullyShredded(Vector &group, idx_t count) {
	auto value = TryGetGroupChild(group, "value");
	if (value) {
		auto value_validity = value->Validity();
		if (!value_validity.CannotHaveNull()) {
			//! Some row may carry a binary overlay - check each one
			for (idx_t i = 0; i < count; i++) {
				if (value_validity.IsValid(i)) {
					return false;
				}
			}
		} else if (count > 0) {
			//! No nulls at all -> every row has a binary overlay -> not fully shredded
			return false;
		}
	}
	auto typed_value = TryGetGroupChild(group, "typed_value");
	if (!typed_value) {
		return true;
	}
	auto &type = typed_value->GetType();
	if (type.id() == LogicalTypeId::STRUCT) {
		//! OBJECT - each field is itself a group
		for (auto &field_group : StructVector::GetEntries(*typed_value)) {
			if (!GroupIsFullyShredded(field_group, count)) {
				return false;
			}
		}
	} else if (type.id() == LogicalTypeId::LIST) {
		//! ARRAY - the element is a group
		if (!GroupIsFullyShredded(ListVector::GetChildMutable(*typed_value), ListVector::GetListSize(*typed_value))) {
			return false;
		}
	}
	return true;
}

static Vector BuildShreddedWrapper(Vector &typed_value, idx_t count);

//! Build the 'typed_value' content of a shredded wrapper from a Parquet 'typed_value' vector
static Vector BuildShreddedTypedContent(Vector &typed_value, idx_t count) {
	auto &type = typed_value.GetType();
	if (type.id() == LogicalTypeId::STRUCT) {
		//! OBJECT - STRUCT(field: group) -> STRUCT(field: wrapper)
		auto &field_entries = StructVector::GetEntries(typed_value);
		auto &field_types = StructType::GetChildTypes(type);
		vector<Vector> wrappers;
		wrappers.reserve(field_entries.size());
		child_list_t<LogicalType> result_children;
		for (idx_t i = 0; i < field_entries.size(); i++) {
			wrappers.push_back(BuildShreddedWrapper(GetGroupChild(field_entries[i], "typed_value"), count));
			result_children.emplace_back(field_types[i].first, wrappers.back().GetType());
		}
		Vector result(LogicalType::STRUCT(std::move(result_children)), MaxValue<idx_t>(count, 1));
		auto &result_entries = StructVector::GetEntries(result);
		for (idx_t i = 0; i < wrappers.size(); i++) {
			result_entries[i].Reference(wrappers[i]);
		}
		//! The node's validity (whether the value is an OBJECT) is the Parquet typed_value struct validity
		auto typed_validity = typed_value.Validity();
		auto &result_validity = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < count; i++) {
			if (!typed_validity.IsValid(i)) {
				result_validity.SetInvalid(i);
			}
		}
		return result;
	}
	if (type.id() == LogicalTypeId::LIST) {
		//! ARRAY - LIST(group) -> LIST(wrapper)
		auto element_count = ListVector::GetListSize(typed_value);
		auto &element_group = ListVector::GetChildMutable(typed_value);
		auto element_wrapper = BuildShreddedWrapper(GetGroupChild(element_group, "typed_value"), element_count);

		auto list_entries = typed_value.Values<list_entry_t>();
		Vector result(LogicalType::LIST(element_wrapper.GetType()), MaxValue<idx_t>(count, 1));
		{
			//! Copy the per-row list entries verbatim - the element child is shared (referenced) below
			VectorWriter<list_entry_t> writer(result, count, 0);
			for (idx_t i = 0; i < count; i++) {
				auto entry = list_entries[i];
				if (entry.IsValid()) {
					writer.WriteValue(entry.GetValue());
				} else {
					writer.WriteNull(list_entry_t(0, 0));
				}
			}
		}
		ListVector::GetChildMutable(result).Reference(element_wrapper);
		ListVector::SetListSize(result, element_count);
		return result;
	}
	//! Primitive - reuse the typed_value vector as-is
	Vector result(type);
	result.Reference(typed_value);
	return result;
}

//! Wrap a Parquet 'typed_value' vector into DuckDB's STRUCT(typed_value, untyped_value_index)
static Vector BuildShreddedWrapper(Vector &typed_value, idx_t count) {
	auto typed_content = BuildShreddedTypedContent(typed_value, count);
	auto typed_validity = typed_value.Validity();

	//! untyped_value_index: NULL where the value is shredded, 0 (the "missing" marker) otherwise. In a
	//! fully-shredded chunk there is never an actual overlay value to point at.
	Vector untyped_value_index(LogicalType::UINTEGER, MaxValue<idx_t>(count, 1));
	VectorScatterWriter<uint32_t> untyped(untyped_value_index);
	for (idx_t i = 0; i < count; i++) {
		if (typed_validity.IsValid(i)) {
			untyped.SetInvalid(i);
		} else {
			untyped[i] = 0;
		}
	}

	child_list_t<LogicalType> wrapper_children;
	wrapper_children.emplace_back("typed_value", typed_content.GetType());
	wrapper_children.emplace_back("untyped_value_index", LogicalType::UINTEGER);
	Vector wrapper(LogicalType::STRUCT(std::move(wrapper_children)), MaxValue<idx_t>(count, 1));
	auto &wrapper_entries = StructVector::GetEntries(wrapper);
	wrapper_entries[0].Reference(typed_content);
	wrapper_entries[1].Reference(untyped_value_index);

	//! The wrapper's own struct validity is only consulted at the very top level (a SQL-NULL row sets the
	//! whole shredded struct to NULL); since 'value' is always NULL in a fully-shredded chunk, a typed-NULL
	//! row is a genuine SQL NULL.
	auto &wrapper_validity = FlatVector::ValidityMutable(wrapper);
	for (idx_t i = 0; i < count; i++) {
		if (!typed_validity.IsValid(i)) {
			wrapper_validity.SetInvalid(i);
		}
	}
	return wrapper;
}

//! Whether the shredded representation of this type matches what the unshredding path would produce.
//! BLOB is excluded: the unshredding path re-encodes a shredded binary value as a base64 VARCHAR
//! (Blob::ToBase64), so emitting it as a native BLOB value would change the observed result.
static bool ShreddedTypeIsEmittable(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BLOB:
		return false;
	case LogicalTypeId::STRUCT:
		for (auto &child : StructType::GetChildTypes(type)) {
			if (!ShreddedTypeIsEmittable(child.second)) {
				return false;
			}
		}
		return true;
	case LogicalTypeId::LIST:
		return ShreddedTypeIsEmittable(ListType::GetChildType(type));
	default:
		return true;
	}
}

bool VariantColumnReader::TryReadShredded(Vector &intermediate_group, idx_t num_values, Vector &result) {
	auto &typed_value = GetGroupChild(intermediate_group, "typed_value");

	//! Only emit a shredded vector if the whole chunk is overlay-free
	if (!GroupIsFullyShredded(intermediate_group, num_values)) {
		return false;
	}
	//! And only if the typed_value layout maps cleanly onto a structured type
	LogicalType structured_type;
	if (!TypedValueLayoutToType(typed_value.GetType(), structured_type)) {
		return false;
	}
	//! And only if every shredded leaf type round-trips identically to the unshredding path
	if (!ShreddedTypeIsEmittable(structured_type)) {
		return false;
	}

	auto shredded_content = BuildShreddedWrapper(typed_value, num_values);

	//! The unshredded (overlay) component is empty - a CONSTANT NULL vector (see ShreddedVector::IsFullyShredded)
	Vector unshredded(LogicalType::VARIANT());
	unshredded.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::SetNull(unshredded, true);

	child_list_t<LogicalType> struct_children;
	struct_children.emplace_back("unshredded", LogicalType::VARIANT());
	struct_children.emplace_back("shredded", shredded_content.GetType());
	Vector shredded_struct(LogicalType::STRUCT(std::move(struct_children)), MaxValue<idx_t>(num_values, 1));
	auto &struct_entries = StructVector::GetEntries(shredded_struct);
	struct_entries[0].Reference(unshredded);
	struct_entries[1].Reference(shredded_content);

	result.Shred(shredded_struct, num_values);
	return true;
}

idx_t VariantColumnReader::Read(ColumnReaderInput &input, Vector &result) {
	if (pending_skips > 0) {
		throw InternalException("VariantColumnReader cannot have pending skips");
	}
	optional_ptr<ColumnReader> typed_value_reader = child_readers.size() == 3 ? child_readers[2].get() : nullptr;

	auto &num_values = input.num_values;
	auto &define_out = input.define_out;
	auto &repeat_out = input.repeat_out;

	// If the child reader values are all valid, "define_out" may not be initialized at all
	// So, we just initialize them to all be valid beforehand
	std::fill_n(define_out, num_values, MaxDefine());

	optional_idx read_count;

	Vector metadata_intermediate(LogicalType::BLOB, num_values);
	Vector intermediate_group(GetIntermediateGroupType(typed_value_reader), num_values);
	auto &group_entries = StructVector::GetEntries(intermediate_group);
	auto &value_intermediate = group_entries[0];

	ColumnReaderInput metadata_reader_input(num_values, define_out, repeat_out);
	auto metadata_values = child_readers[metadata_reader_idx]->Read(metadata_reader_input, metadata_intermediate);

	ColumnReaderInput value_reader_input(num_values, define_out, repeat_out);
	auto value_values = child_readers[value_reader_idx]->Read(value_reader_input, value_intermediate);

	D_ASSERT(child_readers[metadata_reader_idx]->Schema().name == "metadata");
	D_ASSERT(child_readers[value_reader_idx]->Schema().name == "value");

	if (metadata_values != value_values) {
		throw InvalidInputException(
		    "The Variant column did not contain the same amount of values for 'metadata' and 'value'");
	}

	if (typed_value_reader) {
		ColumnReaderInput child_input(num_values, define_out, repeat_out);
		auto typed_values = typed_value_reader->Read(child_input, group_entries[1]);
		if (typed_values != value_values) {
			throw InvalidInputException(
			    "The shredded Variant column did not contain the same amount of values for 'typed_value' and 'value'");
		}
		//! Fast path: if the chunk is fully shredded, emit a SHREDDED_VECTOR directly instead of unshredding
		if (TryReadShredded(intermediate_group, num_values, result)) {
			return value_values;
		}
	}
	vector<VariantValue> intermediate =
	    VariantShreddedConversion::Convert(metadata_intermediate, intermediate_group, 0, num_values, num_values);
	VariantValue::ToVARIANT(intermediate, result);

	read_count = value_values;
	return read_count.GetIndex();
}

void VariantColumnReader::Skip(idx_t num_values) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->Skip(num_values);
	}
}

void VariantColumnReader::RegisterPrefetch(ThriftFileTransport &transport, bool allow_merge) {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		child->RegisterPrefetch(transport, allow_merge);
	}
}

uint64_t VariantColumnReader::TotalCompressedSize() {
	uint64_t size = 0;
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		size += child->TotalCompressedSize();
	}
	return size;
}

idx_t VariantColumnReader::GroupRowsAvailable() {
	for (auto &child : child_readers) {
		if (!child) {
			continue;
		}
		return child->GroupRowsAvailable();
	}
	throw InternalException("No projected columns in struct?");
}

bool VariantColumnReader::TypedValueLayoutToType(const LogicalType &typed_value, LogicalType &output) {
	if (!typed_value.IsNested()) {
		output = typed_value;
		return true;
	}
	auto type_id = typed_value.id();
	if (type_id == LogicalTypeId::STRUCT) {
		//! OBJECT (...)
		auto &object_fields = StructType::GetChildTypes(typed_value);
		child_list_t<LogicalType> children;
		for (auto &object_field : object_fields) {
			auto &name = object_field.first;
			auto &field = object_field.second;
			//! <name>: {
			//! 	value: BLOB,
			//! 	typed_value: <type>
			//! }
			auto &field_children = StructType::GetChildTypes(field);
			idx_t index = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < field_children.size(); i++) {
				if (field_children[i].first == "typed_value") {
					index = i;
					break;
				}
			}
			if (index == DConstants::INVALID_INDEX) {
				//! FIXME: we might be able to just omit this field from the OBJECT, instead of flat-out failing the
				//! conversion No 'typed_value' field, so we can't assign a structured type to this field at all
				return false;
			}
			LogicalType child_type;
			if (!TypedValueLayoutToType(field_children[index].second, child_type)) {
				return false;
			}
			children.emplace_back(name, child_type);
		}
		output = LogicalType::STRUCT(std::move(children));
		return true;
	}
	if (type_id == LogicalTypeId::LIST) {
		//! ARRAY
		auto &element = ListType::GetChildType(typed_value);
		//! element: {
		//! 	value: BLOB,
		//! 	typed_value: <type>
		//! }
		auto &element_children = StructType::GetChildTypes(element);
		idx_t index = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < element_children.size(); i++) {
			if (element_children[i].first == "typed_value") {
				index = i;
				break;
			}
		}
		if (index == DConstants::INVALID_INDEX) {
			//! This *might* be allowed by the spec, it's hard to reason about..
			return false;
		}
		LogicalType child_type;
		if (!TypedValueLayoutToType(element_children[index].second, child_type)) {
			return false;
		}
		output = LogicalType::LIST(child_type);
		return true;
	}
	throw InvalidInputException("VARIANT typed value has to be a primitive/struct/list, not %s",
	                            typed_value.ToString());
}

} // namespace duckdb
