//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/column_index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct ColumnIndex {
    explicit ColumnIndex(idx_t index) : index(index) {
    }

    inline bool operator==(const ColumnIndex &rhs) const {
        return index == rhs.index;
    };
    inline bool operator!=(const ColumnIndex &rhs) const {
        return index != rhs.index;
    };
    inline bool operator<(const ColumnIndex &rhs) const {
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
    const ColumnIndex &GetChildIndex(idx_t idx) const {
        return child_indexes[idx];
    }
    ColumnIndex &GetChildIndex(idx_t idx) {
        return child_indexes[idx];
    }
    bool IsValid() const {
        return index != DConstants::INVALID_INDEX;
    }
    void AddChildIndex(ColumnIndex new_index) {
        this->child_indexes.push_back(std::move(new_index));
    }

private:
    idx_t index;
    vector<ColumnIndex> child_indexes;
};

} // namespace duckdb
