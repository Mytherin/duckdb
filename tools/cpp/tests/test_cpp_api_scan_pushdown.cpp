#include "catch.hpp"
#include "duckdb_cpp.hpp"
#include "duckdb_v2.h"
#include "test_cpp_api.hpp"
#include "test_helpers.hpp"

#include <atomic>
#include <fstream>
#include <mutex>
#include <string>
#include <vector>

// ---------------------------------------------------------------------------
// Stable C++ API tests: what a table function reading files learns about the scan it is part of - the filters pushed
// into it, the statistics it reports, its virtual columns and its metrics - both when it is called directly and when
// a MultiFileFunction wraps it.
//
// Callbacks latch what they observe into file-scope statics rather than asserting in place: a failed REQUIRE inside a
// callback would surface as a query error rather than as the assertion itself.
// ---------------------------------------------------------------------------

namespace {

using namespace duckdb::cxx;

// ---------------------------------------------------------------------------
// cpp_ints(file): reads a text file whose first line names the type of its single column "i" (BIGINT or VARCHAR),
// and whose other lines are its values. The rows are read in blocks of BLOCK_ROWS, and a block whose bounds rule out
// a filter pushed into the scan is skipped without being produced.
// ---------------------------------------------------------------------------

constexpr idx_t BLOCK_ROWS = 10;

struct IntsBind {
	bool is_integer = true;
	std::vector<std::string> values;

	int64_t IntegerAt(idx_t row) const {
		return std::stoll(values[row]);
	}
};

struct IntsGlobal {
	IntsGlobal(std::vector<TableFilter> filters, std::vector<idx_t> blocks, idx_t total_blocks,
	           std::vector<VirtualColumn> virtual_columns)
	    : filters(std::move(filters)), blocks(std::move(blocks)), total_blocks(total_blocks),
	      virtual_columns(std::move(virtual_columns)) {
	}

	std::vector<TableFilter> filters;
	//! The blocks that survived the filters, and the next one to produce
	std::vector<idx_t> blocks;
	idx_t next_block = 0;
	idx_t total_blocks = 0;
	std::atomic<idx_t> blocks_unreported {0};
	//! Where each vector of the output comes from
	std::vector<VirtualColumn> virtual_columns;
};

struct {
	std::mutex lock;
	idx_t init_calls = 0;
	std::vector<idx_t> filter_scan_indexes;
	std::vector<ExpressionType> filter_types;
	std::vector<std::string> filter_constants;
	std::vector<bool> filter_column_refs;

	void Reset() {
		std::lock_guard<std::mutex> guard(lock);
		init_calls = 0;
		filter_scan_indexes.clear();
		filter_types.clear();
		filter_constants.clear();
		filter_column_refs.clear();
	}
} ints_seen;

std::string WriteIntsFile(const std::string &name, const std::string &type, idx_t start, idx_t count) {
	auto path = duckdb::TestCreatePath(name);
	std::ofstream out(path);
	out << type << "\n";
	for (idx_t i = start; i < start + count; i++) {
		out << i << "\n";
	}
	return path;
}

void IntsBind_(TableFunction::BindInput &input) {
	auto &ctx = input.GetContext();
	auto path = GetFilePath(input.GetConstantArgument(0));
	auto fs = ctx.GetFileSystem();
	auto file = fs.OpenFile(path, {FileFlags::READ});
	std::string contents(file.Size(), '\0');
	file.ReadAt(&contents[0], contents.size(), 0);

	IntsBind bind;
	size_t position = 0;
	bool header = true;
	while (position < contents.size()) {
		auto end = contents.find('\n', position);
		auto line = contents.substr(position, end - position);
		position = end == std::string::npos ? contents.size() : end + 1;
		if (header) {
			bind.is_integer = line == "BIGINT";
			header = false;
		} else if (!line.empty()) {
			bind.values.push_back(line);
		}
	}
	input.AddResultColumn("i", ctx.GetFactory().ParseType(bind.is_integer ? "BIGINT" : "VARCHAR"));
	input.SetCardinality(bind.values.size(), true);
	input.SetBindData<IntsBind>(std::move(bind));
}

// The bounds of the rows [start, end) of an integer file
ColumnStatistics IntsBounds(Context &ctx, const IntsBind &bind, idx_t start, idx_t end) {
	ColumnStatistics stats(ctx.GetFactory().ParseType("BIGINT"));
	stats.SetCanHaveNull(false);
	if (start < end) {
		auto min = bind.IntegerAt(start);
		auto max = min;
		for (idx_t row = start; row < end; row++) {
			min = std::min(min, bind.IntegerAt(row));
			max = std::max(max, bind.IntegerAt(row));
		}
		stats.SetMin(ctx.GetFactory().CreateValue(min)).SetMax(ctx.GetFactory().CreateValue(max));
	}
	return stats;
}

void IntsInitGlobal(TableFunction::InitGlobalInput &input) {
	const auto &bind = input.GetBindData<IntsBind>();
	auto &ctx = input.GetContext();
	std::vector<TableFilter> filters;
	{
		std::lock_guard<std::mutex> guard(ints_seen.lock);
		ints_seen.init_calls++;
		for (idx_t i = 0; i < input.GetTableFilterCount(); i++) {
			auto filter = input.GetTableFilter(i);
			auto expression = filter.GetExpression();
			ints_seen.filter_scan_indexes.push_back(filter.GetScanIndex());
			ints_seen.filter_types.push_back(expression.GetType());
			if (expression.GetChildCount() == 2) {
				auto left = expression.GetChild(0);
				auto right = expression.GetChild(1);
				ints_seen.filter_column_refs.push_back(left.GetType() == ExpressionType::BOUND_COLUMN_REF &&
				                                       left.GetColumnIndex() == filter.GetScanIndex());
				if (right.GetType() == ExpressionType::VALUE_CONSTANT) {
					ints_seen.filter_constants.push_back(right.GetConstantValue().ToText());
				}
			}
			filters.push_back(std::move(filter));
		}
	}
	std::vector<VirtualColumn> virtual_columns;
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		virtual_columns.push_back(input.GetVirtualColumn(i));
	}
	const idx_t total_blocks = (bind.values.size() + BLOCK_ROWS - 1) / BLOCK_ROWS;
	std::vector<idx_t> blocks;
	for (idx_t block = 0; block < total_blocks; block++) {
		bool skip = false;
		if (bind.is_integer) {
			auto end = std::min<idx_t>((block + 1) * BLOCK_ROWS, bind.values.size());
			auto stats = IntsBounds(ctx, bind, block * BLOCK_ROWS, end);
			for (auto &filter : filters) {
				// the block bounds only describe "i"
				auto scan_index = filter.GetScanIndex();
				if (input.GetVirtualColumn(scan_index) != VirtualColumn::NONE ||
				    input.GetColumnIndex(scan_index) != 0) {
					continue;
				}
				auto result = filter.CheckStatistics(stats);
				skip = skip || result == FilterPropagateResult::ALWAYS_FALSE ||
				       result == FilterPropagateResult::FALSE_OR_NULL;
			}
		}
		if (!skip) {
			blocks.push_back(block);
		}
	}
	input.SetGlobalState<IntsGlobal>(std::move(filters), std::move(blocks), total_blocks, std::move(virtual_columns));
}

void IntsExec(TableFunction::ExecInput &input) {
	const auto &bind = input.GetBindData<IntsBind>();
	auto &global = input.GetGlobalState<IntsGlobal>();
	auto chunk = input.GetOutputChunk();
	if (global.next_block >= global.blocks.size()) {
		chunk.GetVector(0).SetSize(0);
		return;
	}
	auto block = global.blocks[global.next_block++];
	global.blocks_unreported++;
	auto start = block * BLOCK_ROWS;
	auto count = std::min<idx_t>(BLOCK_ROWS, bind.values.size() - start);
	for (idx_t col = 0; col < input.GetColumnCount(); col++) {
		auto vec = chunk.GetVector(col);
		if (global.virtual_columns[col] == VirtualColumn::FILE_ROW_NUMBER) {
			auto *out = vec.GetDataMutable<int64_t>();
			for (idx_t i = 0; i < count; i++) {
				out[i] = static_cast<int64_t>(start + i);
			}
		} else if (bind.is_integer) {
			auto *out = vec.GetDataMutable<int64_t>();
			for (idx_t i = 0; i < count; i++) {
				out[i] = bind.IntegerAt(start + i);
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				vec.AssignString(i, bind.values[start + i]);
			}
		}
	}
	chunk.GetVector(0).SetSize(count);
}

void IntsStatistics(TableFunction::StatisticsInput &input) {
	const auto &bind = input.GetBindData<IntsBind>();
	auto &ctx = input.GetContext();
	if (input.GetVirtualColumn() == VirtualColumn::FILE_ROW_NUMBER) {
		ColumnStatistics stats(ctx.GetFactory().ParseType("BIGINT"));
		stats.SetCanHaveNull(false).SetMin(ctx.GetFactory().CreateValue(int64_t(0)));
		stats.SetMax(ctx.GetFactory().CreateValue(static_cast<int64_t>(bind.values.size()) - 1));
		input.SetStatistics(stats);
		return;
	}
	if (input.GetColumnIndex() != 0 || !bind.is_integer) {
		return;
	}
	input.SetStatistics(IntsBounds(ctx, bind, 0, bind.values.size()));
}

void IntsMetrics(TableFunction::MetricsInput &input) {
	auto global = input.GetGlobalState<IntsGlobal>();
	if (!global) {
		return;
	}
	input.AddRowGroupsScanned(global->blocks_unreported.exchange(0));
	input.SetTotalRowGroups(global->total_blocks);
}

void RegisterInts(Connection &conn) {
	auto function = TableFunction::Create(conn.GetFactory());
	function.SetName("cpp_ints");
	function.GetSignature().AddParameter("file", conn.GetFactory().CreateType(LogicalTypeId::ANY),
	                                     FunctionParameterKind::POSITIONAL_ONLY);
	function.SetBindCallback(IntsBind_)
	    .SetInitGlobalCallback(IntsInitGlobal)
	    .SetExecCallback(IntsExec)
	    .SetStatisticsCallback(IntsStatistics)
	    .SetMetricsCallback(IntsMetrics)
	    .SetProjectionPushdown(true)
	    .SetTableFilterPushdown(true)
	    .AddVirtualColumn(VirtualColumn::FILE_ROW_NUMBER);
	conn.Register(function);

	auto multi = MultiFileFunction::Create(conn.GetFactory());
	multi.SetName("cpp_read_ints").SetSingleFileFunction("cpp_ints").SetReaderType("Ints").SetFileExtension("ints");
	conn.Register(multi);
}

std::vector<int64_t> CollectBigints(QueryResult result) {
	std::vector<int64_t> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.push_back(view.Data<int64_t>()[view.SelAt(i)]);
		}
	}
	return out;
}

std::vector<std::string> CollectStrings(QueryResult result) {
	std::vector<std::string> out;
	while (auto chunk = result.FetchChunk()) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			REQUIRE(view.IsValid(i));
			out.emplace_back(view.Data<varchar_t>()[view.SelAt(i)].view());
		}
	}
	return out;
}

std::vector<int64_t> Range(int64_t start, int64_t end) {
	std::vector<int64_t> result;
	for (auto i = start; i < end; i++) {
		result.push_back(i);
	}
	return result;
}

//! The row groups the profiled query on the connection reports having scanned, and how many it could have
std::pair<int64_t, int64_t> ScanMetrics(Connection &conn, const std::string &query) {
	auto path = duckdb::TestCreatePath("cpp_ints_profile.json");
	conn.Execute("SET tracked_metrics = ['operator.row_groups_scanned', 'operator.total_row_groups_to_scan']").Drain();
	conn.Execute("PRAGMA enable_profiling = 'json'").Drain();
	conn.Execute("SET profiling_output = '" + path + "'").Drain();
	conn.Execute(query).Drain();
	conn.Execute("PRAGMA disable_profiling").Drain();
	auto result = conn.Execute("SELECT sum(scanned)::BIGINT, max(total)::BIGINT FROM (SELECT "
	                           "unnest(json_extract(content, '$..row_groups_scanned'))::BIGINT AS scanned, "
	                           "unnest(json_extract(content, '$..total_row_groups_to_scan'))::BIGINT AS total FROM "
	                           "read_text('" +
	                           path + "')) WHERE total > 0");
	auto chunk = result.FetchChunk();
	REQUIRE(chunk.GetRowCount() == 1);
	auto scanned = chunk.GetVector(0).GetValue(0);
	auto total = chunk.GetVector(1).GetValue(0);
	return {scanned.Get<int64_t>(), total.Get<int64_t>()};
}

} // namespace

TEST_CASE("Stable C++API: table filters reach a directly called function, which may skip what they rule out",
          "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	auto path = WriteIntsFile("cpp_ints_direct.ints", "BIGINT", 0, 100);

	ints_seen.Reset();
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_ints('" + path + "') WHERE i >= 95")) == Range(95, 100));
	REQUIRE(ints_seen.filter_types == std::vector<ExpressionType> {ExpressionType::COMPARE_GREATERTHANOREQUALTO});
	REQUIRE(ints_seen.filter_scan_indexes == std::vector<idx_t> {0});
	REQUIRE(ints_seen.filter_column_refs == std::vector<bool> {true});
	REQUIRE(ints_seen.filter_constants == std::vector<std::string> {"95"});

	// The engine applies the filters to the blocks the function does produce
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_ints('" + path + "') WHERE i > 41 AND i < 44")) ==
	        std::vector<int64_t> {42, 43});
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_ints('" + path + "') WHERE i % 7 = 0")) ==
	        std::vector<int64_t> {15});
	// ...also when every row a block produces is filtered out
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_ints('" + path + "') WHERE i % 50 = 49")) ==
	        std::vector<int64_t> {49, 99});
	// A filter on a column the query does not otherwise use
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_ints('" + path + "') WHERE i < 5")) ==
	        std::vector<int64_t> {5});

	auto metrics = ScanMetrics(conn, "SELECT i FROM cpp_ints('" + path + "') WHERE i BETWEEN 15 AND 25");
	REQUIRE(metrics == std::pair<int64_t, int64_t>(2, 10));
	metrics = ScanMetrics(conn, "SELECT i FROM cpp_ints('" + path + "')");
	REQUIRE(metrics == std::pair<int64_t, int64_t>(10, 10));
}

TEST_CASE("Stable C++API: table filters reach the single-file function of a multi-file function", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	auto first = WriteIntsFile("cpp_ints_multi_a.ints", "BIGINT", 0, 100);
	auto second = WriteIntsFile("cpp_ints_multi_b.ints", "BIGINT", 100, 100);
	auto glob = duckdb::TestCreatePath("cpp_ints_multi_*.ints");

	ints_seen.Reset();
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_read_ints('" + glob + "') WHERE i >= 195 ORDER BY i")) ==
	        Range(195, 200));
	REQUIRE(ints_seen.init_calls == 2);
	REQUIRE(ints_seen.filter_types.size() == 2);
	REQUIRE(ints_seen.filter_constants == std::vector<std::string> {"195", "195"});

	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_read_ints(['" + first + "', '" + second +
	                                    "']) WHERE i % 10 = 3 AND i > 50")) == std::vector<int64_t> {15});

	auto metrics = ScanMetrics(conn, "SELECT i FROM cpp_read_ints('" + glob + "') WHERE i BETWEEN 15 AND 25");
	REQUIRE(metrics == std::pair<int64_t, int64_t>(2, 20));

	// A filter on the filename is evaluated by the multi-file function, and never reaches the files it skips
	ints_seen.Reset();
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_read_ints('" + glob +
	                                    "', filename = true) WHERE "
	                                    "filename = '" +
	                                    second + "' AND i < 150")) == std::vector<int64_t> {50});
	REQUIRE(ints_seen.init_calls == 1);
}

TEST_CASE("Stable C++API: a filter the files cannot be filtered on in their own type is still applied", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	auto integers = WriteIntsFile("cpp_ints_union_a.ints", "BIGINT", 0, 30);
	auto strings = WriteIntsFile("cpp_ints_union_b.ints", "VARCHAR", 25, 30);

	// "i" is VARCHAR across the two files, while the first produces it as BIGINT
	auto query = "SELECT i FROM cpp_read_ints(['" + integers + "', '" + strings + "'], union_by_name = true) ";
	REQUIRE(CollectStrings(conn.Execute(query + "WHERE i = '27' ORDER BY ALL")) ==
	        std::vector<std::string> {"27", "27"});
	REQUIRE(CollectStrings(conn.Execute(query + "WHERE i LIKE '2_' AND i < '23' ORDER BY ALL")) ==
	        std::vector<std::string> {"20", "21", "22"});
	REQUIRE(CollectStrings(conn.Execute(query + "WHERE i = 'x'")).empty());
}

TEST_CASE("Stable C++API: table function virtual columns", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	auto first = WriteIntsFile("cpp_ints_rownum_a.ints", "BIGINT", 1000, 25);
	auto second = WriteIntsFile("cpp_ints_rownum_b.ints", "VARCHAR", 2000, 5);

	// Only read when named
	auto star = conn.Execute("SELECT * FROM cpp_ints('" + first + "')");
	REQUIRE(star.GetSchema().GetFieldCount() == 1);
	star.Drain();

	REQUIRE(CollectBigints(conn.Execute("SELECT file_row_number FROM cpp_ints('" + first + "') WHERE i >= 1022")) ==
	        std::vector<int64_t> {22, 23, 24});
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_ints('" + first + "') WHERE file_row_number = 3")) ==
	        std::vector<int64_t> {1003});
	REQUIRE(CollectBigints(conn.Execute("SELECT max(file_row_number) FROM cpp_read_ints(['" + first + "', '" + second +
	                                    "'], union_by_name = true)")) == std::vector<int64_t> {24});
	REQUIRE(CollectBigints(conn.Execute("SELECT file_row_number FROM cpp_read_ints(['" + first + "', '" + second +
	                                    "'], union_by_name = true) WHERE i = '2003'")) == std::vector<int64_t> {3});
}

TEST_CASE("Stable C++API: table function statistics reach the optimizer", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	auto first = WriteIntsFile("cpp_ints_stats_a.ints", "BIGINT", 10, 20);
	auto second = WriteIntsFile("cpp_ints_stats_b.ints", "BIGINT", 50, 20);

	auto stats = CollectStrings(conn.Execute("SELECT stats(i)::VARCHAR FROM cpp_ints('" + first + "') LIMIT 1"));
	REQUIRE(stats == std::vector<std::string> {"{'has_no_null': true, 'has_null': false, 'max': 29, 'min': 10}"});

	stats =
	    CollectStrings(conn.Execute("SELECT stats(file_row_number)::VARCHAR FROM cpp_ints('" + first + "') LIMIT 1"));
	REQUIRE(stats == std::vector<std::string> {"{'has_no_null': true, 'has_null': false, 'max': 19, 'min': 0}"});

	// Combined across the files of a multi-file function, when every file is bound up front
	stats = CollectStrings(conn.Execute("SELECT stats(i)::VARCHAR FROM cpp_read_ints(['" + first + "', '" + second +
	                                    "'], union_by_name = true) LIMIT 1"));
	REQUIRE(stats == std::vector<std::string> {"{'has_no_null': true, 'has_null': false, 'max': 69, 'min': 10}"});

	// The optimizer knows no row passes, and the files are never read
	ints_seen.Reset();
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_ints('" + first + "') WHERE i > 100")) ==
	        std::vector<int64_t> {0});
	REQUIRE(ints_seen.init_calls == 0);
}

TEST_CASE("Stable C++API: column statistics and table filter refusals", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	auto &factory = conn.GetFactory();

	ColumnStatistics strings(factory.ParseType("VARCHAR"));
	strings.SetMin(factory.CreateValue(varchar_t("a"))).SetMax(factory.CreateValue(varchar_t("q")));
	// A NULL bound is no bound
	strings.SetMin(Value::CreateNull(factory, factory.ParseType("VARCHAR")));

	ColumnStatistics lists(factory.ParseType("INTEGER[]"));
	lists.SetCanHaveNull(false).SetCanHaveValid(true);
	REQUIRE_THROWS_MATCHES(lists.SetMin(factory.CreateValue(int32_t(1))), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	ColumnStatistics integers(factory.ParseType("INTEGER"));
	REQUIRE_THROWS_MATCHES(integers.SetMin(factory.CreateValue(varchar_t("not a number"))), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(ColumnStatistics(factory.CreateType(LogicalTypeId::ANY)), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));

	// Virtual columns need projection pushdown
	auto function = TableFunction::Create(factory);
	function.SetName("cpp_no_projection").SetBindCallback(IntsBind_).SetExecCallback(IntsExec);
	function.GetSignature().AddParameter("file", factory.CreateType(LogicalTypeId::ANY));
	function.AddVirtualColumn(VirtualColumn::FILE_ROW_NUMBER);
	REQUIRE_THROWS_MATCHES(conn.Register(function), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	REQUIRE_THROWS_MATCHES(function.AddVirtualColumn(VirtualColumn::NONE), Exception,
	                       HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

// ---------------------------------------------------------------------------
// COPY: a format selected by the extension of the file written, and COPY FROM through a multi-file function.
// ---------------------------------------------------------------------------

namespace {

struct IntsWriteState {
	std::string path;
	std::string contents;
};

void IntsWriteInit(CopyFunction::CopyToInitInput &input) {
	input.SetInitData<IntsWriteState>(IntsWriteState {input.GetFilePath(), "BIGINT\n"});
}

std::atomic<idx_t> ints_batches {0};
//! Batch targets for the format, 0 meaning none
idx_t ints_target_bytes = 0;

void IntsWriteBatchSize(CopyFunction::CopyToBatchSizeInput &input) {
	input.SetTarget(1000000);
	if (ints_target_bytes > 0) {
		input.SetTargetBytes(ints_target_bytes);
	}
}

void IntsWriteBatch(CopyFunction::CopyToBatchInput &input) {
	ints_batches++;
	input.SetBatchData<ColumnDataCollection>(input.TakeBatch());
}

void IntsWriteFlush(CopyFunction::CopyToFlushInput &input) {
	auto &state = input.GetInitData<IntsWriteState>();
	auto &batch = input.GetBatchData<ColumnDataCollection>();
	auto &ctx = input.GetContext();
	auto shared = batch.CreateSharedScanState();
	auto worker = batch.CreateWorkerScanState();
	std::vector<LogicalType> types;
	types.push_back(ctx.GetFactory().ParseType("BIGINT"));
	auto chunk = ctx.GetFactory().CreateDataChunk(types);
	while (batch.Scan(shared, worker, chunk)) {
		auto view = chunk.GetVector(0).GetView();
		for (idx_t i = 0; i < chunk.GetRowCount(); i++) {
			state.contents += std::to_string(view.Data<int64_t>()[view.SelAt(i)]) + "\n";
		}
	}
}

void IntsWriteFinalize(CopyFunction::CopyToFinalizeInput &input) {
	auto &state = input.GetInitData<IntsWriteState>();
	auto fs = input.GetContext().GetFileSystem();
	auto file = fs.OpenFile(state.path, {FileFlags::WRITE, FileFlags::FILE_CREATE_NEW});
	file.Write(state.contents.data(), state.contents.size());
}

// The format stores BIGINT only: every column is cast to it
void IntsWriteCast(CopyFunction::CopyToCastInput &input) {
	auto &ctx = input.GetContext();
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		if (input.GetColumnType(i).GetTypeId() != LogicalTypeId::BIGINT) {
			input.SetColumnType(i, ctx.GetFactory().ParseType("BIGINT"));
		}
	}
}

void RegisterIntsCopy(Connection &conn) {
	auto function = CopyFunction::Create(conn.GetFactory());
	function.SetName("ints")
	    .SetFileExtension("ints")
	    .SetCopyToCastCallback(IntsWriteCast)
	    .SetCopyToBatchSizeCallback(IntsWriteBatchSize)
	    .SetCopyToInitCallback(IntsWriteInit)
	    .SetCopyToBatchCallback(IntsWriteBatch)
	    .SetCopyToFlushCallback(IntsWriteFlush)
	    .SetCopyToFinalizeCallback(IntsWriteFinalize)
	    .SetCopyFromMultiFileFunction("cpp_read_ints");
	conn.Register(function);
}

} // namespace

TEST_CASE("Stable C++API: copy function file extension and COPY FROM through a multi-file function", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	RegisterIntsCopy(conn);

	auto first = duckdb::TestCreatePath("cpp_ints_copy_a.ints");
	auto second = duckdb::TestCreatePath("cpp_ints_copy_b.ints");
	// No FORMAT: the extension of the path selects it
	conn.Execute("COPY (SELECT range AS i FROM range(5)) TO '" + first + "'").Drain();
	// The engine casts what the format does not store
	conn.Execute("COPY (SELECT (range + 5)::UTINYINT AS i FROM range(3)) TO '" + second + "'").Drain();
	REQUIRE_THROWS_AS(
	    conn.Execute("COPY (SELECT 'x' AS i) TO '" + duckdb::TestCreatePath("cpp_ints_cast_fails.ints") + "'").Drain(),
	    Exception);
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM cpp_read_ints('" + first + "')")) == Range(0, 5));

	conn.Execute("CREATE TABLE reloaded (i BIGINT)").Drain();
	conn.Execute("COPY reloaded FROM '" + duckdb::TestCreatePath("cpp_ints_copy_*.ints") + "' (FORMAT ints)").Drain();
	REQUIRE(CollectBigints(conn.Execute("SELECT i FROM reloaded ORDER BY i")) == Range(0, 8));

	// Reading the files as the table's columns
	conn.Execute("CREATE TABLE narrowed (i INTEGER)").Drain();
	conn.Execute("COPY narrowed FROM '" + second + "' (FORMAT ints)").Drain();
	REQUIRE(CollectBigints(conn.Execute("SELECT i::BIGINT FROM narrowed ORDER BY i")) == Range(5, 8));

	// Setting both the callbacks and the multi-file function is refused
	auto both = CopyFunction::Create(conn.GetFactory());
	both.SetName("ints_both")
	    .SetCopyFromMultiFileFunction("cpp_read_ints")
	    .SetCopyFromBindCallback([](CopyFunction::CopyFromBindInput &) {})
	    .SetCopyFromExecCallback([](CopyFunction::CopyFromExecInput &) {});
	REQUIRE_THROWS_MATCHES(conn.Register(both), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
	auto missing = CopyFunction::Create(conn.GetFactory());
	missing.SetName("ints_missing").SetCopyFromMultiFileFunction("cpp_read_nothing");
	REQUIRE_THROWS_MATCHES(conn.Register(missing), Exception, HasErrorCode(DUCKDB_V2_ERROR_INPUT_INVALID));
}

TEST_CASE("Stable C++API: copy function batch size in bytes", "[cpp_api]") {
	auto db = Instance(":memory:");
	auto conn = db.Connect();
	RegisterInts(conn);
	RegisterIntsCopy(conn);
	conn.Execute("SET preserve_insertion_order = true").Drain();
	auto path = duckdb::TestCreatePath("cpp_ints_bytes.ints");
	auto copy = "COPY (SELECT range AS i FROM range(10000)) TO '" + path + "'";

	// One batch: the row target is larger than the input
	ints_target_bytes = 0;
	ints_batches = 0;
	conn.Execute(copy).Drain();
	REQUIRE(ints_batches == 1);

	// Cut by bytes as well
	ints_target_bytes = 16 * 1024;
	ints_batches = 0;
	conn.Execute(copy).Drain();
	REQUIRE(ints_batches > 1);

	// A statement's own BATCH_SIZE_BYTES wins
	ints_batches = 0;
	conn.Execute(copy.substr(0, copy.size()) + " (BATCH_SIZE_BYTES '1GB')").Drain();
	REQUIRE(ints_batches == 1);
	ints_target_bytes = 0;
	REQUIRE(CollectBigints(conn.Execute("SELECT count(*) FROM cpp_read_ints('" + path + "')")) ==
	        std::vector<int64_t> {10000});
}
