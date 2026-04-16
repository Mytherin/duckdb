//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector/vector_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/types/string_heap.hpp"
#include <tuple>

namespace duckdb {

//! Tag type for struct vector writers with a fixed set of child types
template <class... Args>
struct FlatStruct {};

template <class T>
struct VectorWriter {
	VectorWriter(Vector &vector, idx_t count)
	    : data(FlatVector::GetDataMutable<T>(vector)), validity(FlatVector::Validity(vector)), count(count) {
	}

	void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
	}

	T &operator[](idx_t idx) {
		D_ASSERT(idx < count);
		return data[idx];
	}

private:
	T *data;
	ValidityMask &validity;
	idx_t count;
};

template <>
struct VectorWriter<string_t> {
	struct StringElement {
		StringElement(VectorWriter<string_t> &writer, string_t *data, idx_t idx)
		    : writer(writer), data(data), idx(idx) {
		}

		//! Constructs an empty string of a given length and returns it
		//! Note: the empty string must be filled and .Finalize() must be called on it
		inline string_t &EmptyString(idx_t length) {
			if (length <= string_t::INLINE_LENGTH) {
				data[idx] = string_t(UnsafeNumericCast<uint32_t>(length));
			} else {
				auto &heap = writer.GetHeap();
				data[idx] = heap.CreateEmptyStringInHeap(length);
			}
			return data[idx];
		}
		inline string_t &operator=(string_t val) {
			if (val.IsInlined()) {
				data[idx] = val;
			} else {
				auto &heap = writer.GetHeap();
				data[idx] = heap.AddBlobToHeap(val.GetData(), val.GetSize());
			}
			return data[idx];
		}
		inline void AssignWithoutCopying(string_t val) {
			data[idx] = val;
		}
		inline char *GetDataWriteable() {
			return data[idx].GetDataWriteable();
		}
		inline void Finalize() {
			data[idx].Finalize();
		}
		inline string GetString() {
			return data[idx].GetString();
		}

		operator string_t() const { // NOLINT: allow implicit conversion
			return data[idx];
		}

	private:
		VectorWriter<string_t> &writer;
		string_t *data;
		idx_t idx;
	};

	VectorWriter(Vector &vector, idx_t count);

	inline void SetInvalid(idx_t idx) {
		D_ASSERT(idx < count);
		validity.SetInvalid(idx);
	}

	inline StringElement operator[](idx_t idx) {
		D_ASSERT(idx < count);
		return StringElement(*this, data, idx);
	}

	inline StringHeap &GetHeap() {
		if (!heap) {
			InitializeHeap();
		}
		return *heap;
	}

private:
	void InitializeHeap();

private:
	Vector &vector;
	string_t *data;
	ValidityMask &validity;
	optional_ptr<StringHeap> heap;
	idx_t count;
};

//===--------------------------------------------------------------------===//
// FlatStruct VectorWriter helpers (specialization in struct_vector_writer.hpp)
//===--------------------------------------------------------------------===//

//! Children container for struct writers - supports Get<N>() and ForEachChild()
template <class... Args>
struct StructWriterChildren {
	template <size_t... Is>
	StructWriterChildren(vector<Vector> &entries, idx_t count, std::index_sequence<Is...>)
	    : data(VectorWriter<Args>(entries[Is], count)...) {
	}

	template <size_t N>
	auto &Get() {
		return std::get<N>(data);
	}
	template <size_t N>
	const auto &Get() const {
		return std::get<N>(data);
	}

	//! Calls f(child_writer) for each child in order — useful for iterating over all children
	template <class F>
	void ForEachChild(F &&f) {
		ForEachChildImpl(std::forward<F>(f), std::index_sequence_for<Args...> {});
	}

	void SetInvalid(idx_t idx) {
		SetInvalidImpl(idx, std::index_sequence_for<Args...> {});
	}

	std::tuple<VectorWriter<Args>...> data;

private:
	template <class F, size_t... Is>
	void ForEachChildImpl(F &&f, std::index_sequence<Is...>) {
		(f(std::get<Is>(data)), ...);
	}

	template <size_t... Is>
	void SetInvalidImpl(idx_t idx, std::index_sequence<Is...>) {
		(std::get<Is>(data).SetInvalid(idx), ...);
	}
};

} // namespace duckdb
