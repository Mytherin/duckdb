//===----------------------------------------------------------------------===//
//                         DuckDB
//
// shell_explain_renderer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "shell_renderer.hpp"

namespace duckdb_shell {

//! Creates the renderer used for EXPLAIN / EXPLAIN ANALYZE output
unique_ptr<ShellRenderer> CreateExplainRenderer(ShellState &state);

} // namespace duckdb_shell
