//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/stable/vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/stable/common.hpp"
#include "duckdb/stable/vector.hpp"

namespace duckdb_stable {

class Vector {
public:
	Vector(duckdb_vector vec_p, bool owning = false) : vec(vec_p), owning(owning) {
	}
	~Vector() {
		if (vec && owning) {
			duckdb_destroy_vector(&vec);
		}
	}

	duckdb_vector c_vec() {
		return vec;
	}

private:
	duckdb_vector vec;
	bool owning;
};

} // namespace duckdb_stable
