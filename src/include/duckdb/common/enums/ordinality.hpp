//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/ordinality.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {

enum class Ordinality : uint8_t { NO_ORDINALITY = 0, WITH_ORDINALITY = 1 };

} // namespace duckdb
