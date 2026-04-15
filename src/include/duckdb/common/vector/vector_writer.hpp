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
#include <array>
#include <tuple>
#include <type_traits>

namespace duckdb {

//! Tag type for struct vector writers with a fixed set of child types
template <class... Args>
struct FixedStruct {};

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
// FixedStruct VectorWriter helpers (specialization in struct_vector_writer.hpp)
//===--------------------------------------------------------------------===//

//! Helper to check if all types in a parameter pack are the same
template <class...>
struct AllSameType : std::true_type {};
template <class T>
struct AllSameType<T> : std::true_type {};
template <class T, class U, class... Rest>
struct AllSameType<T, U, Rest...>
    : std::conditional_t<std::is_same<T, U>::value, AllSameType<U, Rest...>, std::false_type> {};

//! Children container for homogeneous struct writers (all children same type) - supports operator[]
template <bool HOMOGENEOUS, class... Args>
struct StructWriterChildren;

template <class T, class... Rest>
struct StructWriterChildren<true, T, Rest...> {
	static constexpr idx_t SIZE = 1 + sizeof...(Rest);

	template <size_t... Is>
	StructWriterChildren(vector<Vector> &entries, idx_t count, std::index_sequence<Is...>)
	    : data {{VectorWriter<T>(entries[Is], count)...}} {
	}

	VectorWriter<T> &operator[](idx_t idx) {
		return data[idx];
	}
	const VectorWriter<T> &operator[](idx_t idx) const {
		return data[idx];
	}

	void SetInvalid(idx_t idx) {
		for (idx_t i = 0; i < SIZE; i++) {
			data[i].SetInvalid(idx);
		}
	}

	std::array<VectorWriter<T>, SIZE> data;
};

//! Children container for heterogeneous struct writers (mixed child types) - supports Get<N>()
template <class... Args>
struct StructWriterChildren<false, Args...> {
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

	void SetInvalid(idx_t idx) {
		SetInvalidImpl(idx, std::index_sequence_for<Args...> {});
	}

	std::tuple<VectorWriter<Args>...> data;

private:
	template <size_t... Is>
	void SetInvalidImpl(idx_t idx, std::index_sequence<Is...>) {
		int dummy[] = {(std::get<Is>(data).SetInvalid(idx), 0)...};
		(void)dummy;
	}
};

} // namespace duckdb
