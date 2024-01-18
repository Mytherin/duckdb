//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/physical_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct PhysicalIndex {
	explicit PhysicalIndex(idx_t index) : index(index) {
	}
	PhysicalIndex(idx_t index, vector<PhysicalIndex> child_indexes_p)
	    : index(index), child_indexes(std::move(child_indexes_p)) {
	}

	idx_t index;

	inline bool operator==(const PhysicalIndex &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const PhysicalIndex &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const PhysicalIndex &rhs) const {
		return index < rhs.index;
	};
	idx_t GetPrimaryIndex() const {
		return index;
	}
	bool HasChildren() const {
		return !child_indexes.empty();
	}
	idx_t ChildIndexCount() const {
		return child_indexes.size();
	}
	const PhysicalIndex &GetChildIndex(idx_t idx) const {
		return child_indexes[idx];
	}
	PhysicalIndex &GetChildIndex(idx_t idx) {
		return child_indexes[idx];
	}
	const vector<PhysicalIndex> &GetChildIndexes() {
		return child_indexes;
	}
	void AddChildIndex(PhysicalIndex new_index) {
		this->child_indexes.push_back(std::move(new_index));
	}
	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
    void Serialize(Serializer &serializer) const;
    static PhysicalIndex Deserialize(Deserializer &deserializer);

private:
	vector<PhysicalIndex> child_indexes;
};

} // namespace duckdb
