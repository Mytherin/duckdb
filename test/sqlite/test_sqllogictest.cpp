#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/extension/generated_extension_loader.hpp"
#include "duckdb/parser/parser.hpp"
#include "sqllogic_test_runner.hpp"
#include "test_helpers.hpp"
#include "test_config.hpp"

#include <functional>
#include <string>
#include <vector>

using namespace duckdb;
using namespace std;

// code below traverses the test directory and makes individual test cases out
// of each script
static void listFiles(FileSystem &fs, const string &path, std::function<void(const string &)> cb) {
	fs.ListFiles(path, [&](string fname, bool is_dir) {
		string full_path = fs.JoinPath(path, fname);
		if (is_dir) {
			// recurse into directory
			listFiles(fs, full_path, cb);
		} else {
			cb(full_path);
		}
	});
}

static bool endsWith(const string &mainStr, const string &toMatch) {
	return (mainStr.size() >= toMatch.size() &&
	        mainStr.compare(mainStr.size() - toMatch.size(), toMatch.size(), toMatch) == 0);
}

template <bool VERIFICATION, bool AUTO_SWITCH_TEST_DIR = false>
static void testRunner() {
	// this is an ugly hack that uses the test case name to pass the script file
	// name if someone has a better idea...
	auto name = Catch::getResultCapture().getCurrentTestName();

	auto &test_config = TestConfiguration::Get();

	string initial_dbpath = test_config.GetInitialDBPath();
	test_config.ProcessPath(initial_dbpath, name);
	if (!initial_dbpath.empty()) {
		auto test_path = StringUtil::Replace(initial_dbpath, TestDirectoryPath(), string());
		test_path = StringUtil::Replace(test_path, "\\", "/");
		auto components = StringUtil::Split(test_path, "/");
		components.pop_back();
		string total_path = TestDirectoryPath();
		for (auto &component : components) {
			if (component.empty()) {
				continue;
			}
			total_path = TestJoinPath(total_path, component);
			TestCreateDirectory(total_path);
		}
	}
	SQLLogicTestRunner runner(std::move(initial_dbpath));
	runner.output_sql = Catch::getCurrentContext().getConfig()->outputSQL();
	runner.enable_verification = VERIFICATION;

	string prev_directory;

	// We assume the test working dir for extensions to be one dir above the test/sql. Note that this is very hacky.
	// however for now it suffices: we use it to run tests from out-of-tree extensions that are based on the extension
	// template which adheres to this convention.
	if (AUTO_SWITCH_TEST_DIR) {
		prev_directory = TestGetCurrentDirectory();

		std::size_t found = name.rfind("test/sql");
		if (found == std::string::npos) {
			throw InvalidInputException("Failed to auto detect working dir for test '" + name +
			                            "' because a non-standard path was used!");
		}
		auto test_working_dir = name.substr(0, found);

		// Parse the test dir automatically
		TestChangeDirectory(test_working_dir);
	}
	try {
		runner.ExecuteFile(name);
	} catch (...) {
		// This is to allow cleanup to be executed, failure is already logged
	}

	if (AUTO_SWITCH_TEST_DIR) {
		TestChangeDirectory(prev_directory);
	}

	// clear test directory after running tests
	ClearTestDirectory();
}

static string ParseGroupFromPath(string file) {
	string extension = "";
	if (file.find(".test_slow") != std::string::npos) {
		// "slow" in the name indicates a slow test (i.e. only run as part of allunit)
		extension = "[.]";
	}
	if (file.find(".test_coverage") != std::string::npos) {
		// "coverage" in the name indicates a coverage test (i.e. only run as part of coverage)
		return "[coverage][.]";
	}
	// move backwards to the last slash
	int group_begin = -1, group_end = -1;
	for (idx_t i = file.size(); i > 0; i--) {
		if (file[i - 1] == '/' || file[i - 1] == '\\') {
			if (group_end == -1) {
				group_end = i - 1;
			} else {
				group_begin = i;
				return "[" + file.substr(group_begin, group_end - group_begin) + "]" + extension;
			}
		}
	}
	if (group_end == -1) {
		return "[" + file + "]" + extension;
	}
	return "[" + file.substr(0, group_end) + "]" + extension;
}

namespace duckdb {

void RegisterSqllogictests() {
	vector<string> enable_verification_excludes = {
	    // too slow for verification
	    "test/select5.test",
	    "test/index",
	    // optimization masks int32 overflow
	    "test/random/aggregates/slt_good_102.test",
	    "test/random/aggregates/slt_good_11.test",
	    "test/random/aggregates/slt_good_115.test",
	    "test/random/aggregates/slt_good_116.test",
	    "test/random/aggregates/slt_good_118.test",
	    "test/random/aggregates/slt_good_119.test",
	    "test/random/aggregates/slt_good_122.test",
	    "test/random/aggregates/slt_good_17.test",
	    "test/random/aggregates/slt_good_20.test",
	    "test/random/aggregates/slt_good_23.test",
	    "test/random/aggregates/slt_good_25.test",
	    "test/random/aggregates/slt_good_3.test",
	    "test/random/aggregates/slt_good_30.test",
	    "test/random/aggregates/slt_good_31.test",
	    "test/random/aggregates/slt_good_38.test",
	    "test/random/aggregates/slt_good_39.test",
	    "test/random/aggregates/slt_good_4.test",
	    "test/random/aggregates/slt_good_43.test",
	    "test/random/aggregates/slt_good_46.test",
	    "test/random/aggregates/slt_good_51.test",
	    "test/random/aggregates/slt_good_56.test",
	    "test/random/aggregates/slt_good_66.test",
	    "test/random/aggregates/slt_good_7.test",
	    "test/random/aggregates/slt_good_72.test",
	    "test/random/aggregates/slt_good_82.test",
	    "test/random/aggregates/slt_good_84.test",
	    "test/random/aggregates/slt_good_85.test",
	    "test/random/aggregates/slt_good_91.test",
	    "test/random/expr/slt_good_15.test",
	    "test/random/expr/slt_good_66.test",
	    "test/random/expr/slt_good_91.test",
	};
	vector<string> excludes = {
	    // tested separately
	    "test/select1.test", "test/select2.test", "test/select3.test", "test/select4.test",
	    // feature not supported
	    "evidence/slt_lang_replace.test",       // INSERT OR REPLACE
	    "evidence/slt_lang_reindex.test",       // REINDEX
	    "evidence/slt_lang_update.test",        // Multiple assignments to same column "x" in update
	    "evidence/slt_lang_createtrigger.test", // TRIGGER
	    "evidence/slt_lang_droptrigger.test",   // TRIGGER
	                                            // no + for varchar columns
	    "test/index/random/10/slt_good_14.test", "test/index/random/10/slt_good_1.test",
	    "test/index/random/10/slt_good_0.test", "test/index/random/10/slt_good_12.test",
	    "test/index/random/10/slt_good_6.test", "test/index/random/10/slt_good_13.test",
	    "test/index/random/10/slt_good_5.test", "test/index/random/10/slt_good_10.test",
	    "test/index/random/10/slt_good_11.test", "test/index/random/10/slt_good_4.test",
	    "test/index/random/10/slt_good_8.test", "test/index/random/10/slt_good_3.test",
	    "test/index/random/10/slt_good_2.test", "test/index/random/100/slt_good_1.test",
	    "test/index/random/100/slt_good_0.test", "test/index/random/1000/slt_good_0.test",
	    "test/index/random/1000/slt_good_7.test", "test/index/random/1000/slt_good_6.test",
	    "test/index/random/1000/slt_good_5.test", "test/index/random/1000/slt_good_8.test",
	    // overflow in 32-bit integer multiplication (sqlite does automatic upcasting)
	    "test/random/aggregates/slt_good_96.test", "test/random/aggregates/slt_good_75.test",
	    "test/random/aggregates/slt_good_64.test", "test/random/aggregates/slt_good_9.test",
	    "test/random/aggregates/slt_good_110.test", "test/random/aggregates/slt_good_101.test",
	    "test/random/expr/slt_good_55.test", "test/random/expr/slt_good_115.test", "test/random/expr/slt_good_103.test",
	    "test/random/expr/slt_good_80.test", "test/random/expr/slt_good_75.test", "test/random/expr/slt_good_42.test",
	    "test/random/expr/slt_good_49.test", "test/random/expr/slt_good_24.test", "test/random/expr/slt_good_30.test",
	    "test/random/expr/slt_good_8.test", "test/random/expr/slt_good_61.test",
	    // dependencies between tables/views prevent dropping in DuckDB without CASCADE
	    "test/index/view/1000/slt_good_0.test", "test/index/view/100/slt_good_0.test",
	    "test/index/view/100/slt_good_5.test", "test/index/view/100/slt_good_1.test",
	    "test/index/view/100/slt_good_3.test", "test/index/view/100/slt_good_4.test",
	    "test/index/view/100/slt_good_2.test", "test/index/view/10000/slt_good_0.test",
	    "test/index/view/10/slt_good_5.test", "test/index/view/10/slt_good_7.test",
	    "test/index/view/10/slt_good_1.test", "test/index/view/10/slt_good_3.test",
	    "test/index/view/10/slt_good_4.test", "test/index/view/10/slt_good_6.test",
	    "test/index/view/10/slt_good_2.test",
	    // strange error in hash comparison, results appear correct...
	    "test/index/random/10/slt_good_7.test", "test/index/random/10/slt_good_9.test"};
	duckdb::unique_ptr<FileSystem> fs = FileSystem::CreateLocal();
	listFiles(*fs, fs->JoinPath(fs->JoinPath("third_party", "sqllogictest"), "test"), [&](const string &path) {
		if (endsWith(path, ".test")) {
			for (auto &excl : excludes) {
				if (path.find(excl) != string::npos) {
					return;
				}
			}
			bool enable_verification = true;
			for (auto &excl : enable_verification_excludes) {
				if (path.find(excl) != string::npos) {
					enable_verification = false;
					break;
				}
			}
			if (enable_verification) {
				REGISTER_TEST_CASE(testRunner<true>, StringUtil::Replace(path, "\\", "/"), "[sqlitelogic][.]");
			} else {
				REGISTER_TEST_CASE(testRunner<false>, StringUtil::Replace(path, "\\", "/"), "[sqlitelogic][.]");
			}
		}
	});
	listFiles(*fs, "test", [&](const string &path) {
		if (endsWith(path, ".test") || endsWith(path, ".test_slow") || endsWith(path, ".test_coverage")) {
			// parse the name / group from the test
			REGISTER_TEST_CASE(testRunner<false>, StringUtil::Replace(path, "\\", "/"), ParseGroupFromPath(path));
		}
	});

#if defined(GENERATED_EXTENSION_HEADERS) && GENERATED_EXTENSION_HEADERS && !defined(DUCKDB_AMALGAMATION)
	for (const auto &extension_test_path : LoadedExtensionTestPaths()) {
		listFiles(*fs, extension_test_path, [&](const string &path) {
			if (endsWith(path, ".test") || endsWith(path, ".test_slow") || endsWith(path, ".test_coverage")) {
				auto fun = testRunner<false, true>;
				REGISTER_TEST_CASE(fun, StringUtil::Replace(path, "\\", "/"), ParseGroupFromPath(path));
			}
		});
	}
#endif
}
} // namespace duckdb
