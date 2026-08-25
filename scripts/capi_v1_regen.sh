#!/usr/bin/env bash
# Regenerates the V1 C API headers from api_spec/v1/, then formats the outputs.
# Invoked manually after editing YAML, or via make generate-files. Expects `capigen` on PATH.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

if ! command -v capigen >/dev/null 2>&1; then
	echo "error: capigen is not on PATH - run 'make generate-files-deps' to install the generator dependencies" >&2
	exit 1
fi

# the formatter runs from the repository's format venv when make provides it (see the format_venv target)
FORMAT_PYTHON="${FORMAT_PYTHON:-python3}"

capigen c \
	--spec-dir api_spec/v1 \
	-o src/include/duckdb.h

capigen extension_header \
	--spec-dir api_spec/v1 \
	--template api_spec/v1/extension/duckdb_extension.h.in \
	--internal-out src/include/duckdb/main/capi/extension_api.hpp \
	-o src/include/duckdb_extension.h

"$FORMAT_PYTHON" scripts/format.py src/include/duckdb.h --fix --noconfirm
"$FORMAT_PYTHON" scripts/format.py src/include/duckdb_extension.h --fix --noconfirm
"$FORMAT_PYTHON" scripts/format.py src/include/duckdb/main/capi/extension_api.hpp --fix --noconfirm
