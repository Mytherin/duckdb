#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/thread.hpp"

#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace duckdb;

//===--------------------------------------------------------------------===//
// Query log structs and parsing
//===--------------------------------------------------------------------===//

enum class OpType { SELECT, MERGE, BEGIN, COMMIT, ROLLBACK, OTHER };

struct QueryRecord {
	idx_t connection; // index into connection_names
	OpType op;
	string table;
	// SELECT: params[0] = id (integer)
	// MERGE:  params[0] = id (integer), params[1] = sk (integer),
	//         params[2] = val (string),  params[3] = update_val (string)
	vector<string> params;
};

static OpType ParseOpType(const string &s) {
	if (s == "SELECT") {
		return OpType::SELECT;
	}
	if (s == "MERGE") {
		return OpType::MERGE;
	}
	if (s == "BEGIN") {
		return OpType::BEGIN;
	}
	if (s == "COMMIT") {
		return OpType::COMMIT;
	}
	if (s == "ROLLBACK") {
		return OpType::ROLLBACK;
	}
	return OpType::OTHER;
}

// Strip surrounding double-quotes from a TSV field if present.
static string StripQuotes(const string &s) {
	if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
		return s.substr(1, s.size() - 2);
	}
	return s;
}

static vector<QueryRecord> ParseQueryLog(const string &path, vector<string> &connection_names) {
	vector<QueryRecord> records;
	std::ifstream file(path);
	if (!file.is_open()) {
		return records;
	}

	// name -> index, built in insertion order
	std::map<string, idx_t> conn_index;

	string line;
	getline(file, line); // skip header: connection\top\ttable\tparams...

	while (getline(file, line)) {
		if (line.empty()) {
			continue;
		}

		vector<string> fields;
		std::stringstream ss(line);
		string field;
		while (getline(ss, field, '\t')) {
			fields.push_back(StripQuotes(field));
		}

		if (fields.size() < 3) {
			continue;
		}

		const string &conn_str = fields[0];
		auto it = conn_index.find(conn_str);
		if (it == conn_index.end()) {
			idx_t idx = connection_names.size();
			conn_index[conn_str] = idx;
			connection_names.push_back(conn_str);
			it = conn_index.find(conn_str);
		}

		QueryRecord rec;
		rec.connection = it->second;
		rec.op = ParseOpType(fields[1]);
		rec.table = fields[2];
		for (idx_t i = 3; i < fields.size(); i++) {
			rec.params.push_back(fields[i]);
		}
		records.push_back(rec);
	}
	return records;
}

//===--------------------------------------------------------------------===//
// Write-set oracle: tracks every token ever written to (table, id)
//===--------------------------------------------------------------------===//

// Maps (table_name, row_id) -> set of individual value tokens that some MERGE
// wrote, either as the initial insert val or as a CONCAT update_val.
// Built once from the full log before any threads run; read-only during replay.
using RowKey = std::pair<string, int32_t>;
using AllowedValues = std::map<RowKey, std::unordered_set<string>>;

static AllowedValues BuildAllowedValues(const vector<QueryRecord> &records) {
	AllowedValues allowed;
	for (auto &rec : records) {
		if (rec.op != OpType::MERGE || rec.params.size() != 4) {
			continue;
		}
		RowKey key = {rec.table, std::stoi(rec.params[0])};
		// params[2]: val used when the row is inserted for the first time
		allowed[key].insert(rec.params[2]);
		// params[3]: update_val appended via CONCAT when the row already exists
		allowed[key].insert(rec.params[3]);
	}
	return allowed;
}

//===--------------------------------------------------------------------===//
// Per-connection stream execution
//===--------------------------------------------------------------------===//

static void RunConnectionStream(DuckDB *db, const vector<QueryRecord> &records, idx_t connection_id,
                                const AllowedValues &allowed) {
	Connection con(*db);

	for (auto &rec : records) {
		switch (rec.op) {
		case OpType::SELECT: {
			// select val from <table> where id = ?
			if (rec.params.size() != 1) {
				fprintf(stderr, "Wrong parameter count for SELECT");
				FAIL();
			}
			const string sql = "SELECT val FROM " + rec.table + " WHERE id = ?";
			auto id = std::stoi(rec.params[0]);
			auto result = con.Query(sql, id);
			if (result->HasError()) {
				break;
			}
			// Verify every comma-separated token in the returned val was
			// actually written by some MERGE for this (table, id).
			auto &mat = result->Cast<MaterializedQueryResult>();
			RowKey key = {rec.table, id};
			auto allowed_it = allowed.find(key);
			for (idx_t row = 0; row < mat.RowCount(); row++) {
				auto v = mat.GetValue(0, row);
				if (v.IsNull()) {
					fprintf(stderr, "Value should not have NULLs");
					FAIL();
				}
				std::stringstream tok_ss(v.ToString());
				string token;
				while (std::getline(tok_ss, token, ',')) {
					bool ok = allowed_it != allowed.end() && allowed_it->second.count(token);
					if (!ok) {
						fprintf(stderr,
						        "[conn=%llu] Assert failed: No transaction wrote (%s, %d) token '%s'\n",
						        connection_id, rec.table.c_str(), id, token.c_str());
						FAIL();
					}
				}
			}
			break;
		}
		case OpType::MERGE: {
			// merge into <table> as t
			//   using (select ? as id, ? as sk, ? as val) as upserts
			//   on t.id = upserts.id
			//   when matched then update set val = concat(t.val, ',', ?)
			//   when not matched then insert
			if (rec.params.size() != 4) {
				fprintf(stderr, "Wrong parameter count for MERGE");
				FAIL();
			}
			const string sql = "MERGE INTO " + rec.table +
			                   " AS t"
			                   " USING (SELECT ? AS id, ? AS sk, ? AS val) AS upserts"
			                   " ON t.id = upserts.id"
			                   " WHEN MATCHED THEN UPDATE SET val = CONCAT(t.val, ',', ?)"
			                   " WHEN NOT MATCHED THEN INSERT";
			auto id = std::stoi(rec.params[0]);
			auto sk = std::stoi(rec.params[1]);
			auto &val = rec.params[2];
			auto &update_val = rec.params[3];
			con.Query(sql, id, sk, val, update_val);
			break;
		}
		case OpType::BEGIN:
			con.Query("BEGIN");
			break;
		case OpType::COMMIT:
			con.Query("COMMIT");
			break;
		case OpType::ROLLBACK:
			con.Query("ROLLBACK");
			break;
		default:
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Test
//===--------------------------------------------------------------------===//

TEST_CASE("Replay jepsen logs", "[stream_replay]") {
	const string tsv_path = "20260318T141715/queries.tsv";

	vector<string> connection_names;
	auto records = ParseQueryLog(tsv_path, connection_names);
	INFO("Could not parse any records from " + tsv_path);
	REQUIRE(!records.empty());

	// Group queries by connection index; connection_names already has unique names in insertion order.
	vector<vector<QueryRecord>> conn_records(connection_names.size());
	for (auto &rec : records) {
		conn_records[rec.connection].push_back(rec);
	}

	auto db_name = TestCreatePath("jepsen.db");
	DuckDB db(db_name);
	{
		Connection setup(db);
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE txn0 (id INTEGER NOT NULL PRIMARY KEY, sk INTEGER, val VARCHAR)"));
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE txn1 (id INTEGER NOT NULL PRIMARY KEY, sk INTEGER, val VARCHAR)"));
		REQUIRE_NO_FAIL(setup.Query("CREATE TABLE txn2 (id INTEGER NOT NULL PRIMARY KEY, sk INTEGER, val VARCHAR)"));
	}

	// Build the write-set oracle from the full log before any thread runs.
	auto allowed = BuildAllowedValues(records);

	// One thread per unique connection identifier.
	vector<thread> threads;
	threads.reserve(connection_names.size());
	for (idx_t i = 0; i < connection_names.size(); i++) {
		threads.emplace_back(RunConnectionStream, &db, conn_records[i], i, std::cref(allowed));
	}
	for (auto &t : threads) {
		t.join();
	}

	// Sanity: all tables remain readable after the concurrent workload.
	Connection verify(db);
	REQUIRE_NO_FAIL(*verify.Query("SELECT COUNT(*) FROM txn0"));
	REQUIRE_NO_FAIL(*verify.Query("SELECT COUNT(*) FROM txn1"));
	REQUIRE_NO_FAIL(*verify.Query("SELECT COUNT(*) FROM txn2"));
}
