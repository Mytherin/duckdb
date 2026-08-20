#include "catch.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binding_alias.hpp"

using namespace duckdb;

static QualifiedName MakeName(duckdb::vector<Identifier> path) {
	auto name = std::move(path.back());
	path.pop_back();
	return QualifiedName(std::move(path), std::move(name));
}

TEST_CASE("Test QualifiedName comparison", "[qualified_name]") {
	SECTION("catalog/schema/name") {
		REQUIRE(QualifiedName("c", "s", "t") == QualifiedName("c", "s", "t"));
		REQUIRE(QualifiedName("c", "s", "t") != QualifiedName("c2", "s", "t"));
		REQUIRE(QualifiedName("c", "s", "t") != QualifiedName("c", "s2", "t"));
		REQUIRE(QualifiedName("c", "s", "t") != QualifiedName("c", "s", "t2"));
		// a schema-qualified name is not equal to an unqualified one
		REQUIRE(QualifiedName(Identifier(), "s", "t") != QualifiedName("t"));
		// identifiers are case insensitive
		REQUIRE(QualifiedName("C", "S", "T") == QualifiedName("c", "s", "t"));
		REQUIRE(QualifiedName("C", "S", "T").Hash() == QualifiedName("c", "s", "t").Hash());
	}

	SECTION("nested schemas") {
		// the qualification can be deeper than [catalog, schema] - every component must be compared
		auto nested = MakeName({"c", "s1", "s2", "t"});
		REQUIRE(nested == MakeName({"c", "s1", "s2", "t"}));
		REQUIRE(nested != MakeName({"c", "s3", "s2", "t"}));
		REQUIRE(nested != MakeName({"c", "s2", "t"}));
		REQUIRE(nested != MakeName({"s1", "s2", "t"}));
		REQUIRE(nested.Hash() == MakeName({"c", "s1", "s2", "t"}).Hash());
		REQUIRE(nested.Hash() != MakeName({"c", "s3", "s2", "t"}).Hash());
		REQUIRE(nested.Hash() != MakeName({"c", "s2", "t"}).Hash());
	}

	SECTION("catalog qualification") {
		auto nested = MakeName({"c", "s1", "s2", "t"});
		// replacing the catalog keeps the nested schema path
		REQUIRE(nested.WithCatalog("c2") == MakeName({"c2", "s1", "s2", "t"}));
		REQUIRE(QualifiedName(Identifier(), "s", "t").WithCatalog("c") == QualifiedName("c", "s", "t"));
		// an empty catalog strips the catalog qualification
		REQUIRE(nested.WithCatalog(Identifier()) == MakeName({"s1", "s2", "t"}));
		REQUIRE(QualifiedName("c", Identifier(), "t").WithCatalog(Identifier()) == QualifiedName("t"));
		REQUIRE(QualifiedName("t").WithCatalog("c") == QualifiedName("c", Identifier(), "t"));
	}

	SECTION("parsing") {
		REQUIRE(QualifiedName::Parse("t") == QualifiedName("t"));
		REQUIRE(QualifiedName::Parse("s.t") == QualifiedName(Identifier(), "s", "t"));
		REQUIRE(QualifiedName::Parse("c.s.t") == QualifiedName("c", "s", "t"));
		// a name can be qualified with a nested schema path
		REQUIRE(QualifiedName::Parse("c.s1.s2.t") == MakeName({"c", "s1", "s2", "t"}));
		REQUIRE(QualifiedName::Parse("\"c\".\"s1\".\"s2\".\"t\"") == MakeName({"c", "s1", "s2", "t"}));
		REQUIRE(QualifiedName::Parse("c.s1.s2.t").ToString() == "c.s1.s2.t");
	}
}

TEST_CASE("Test BindingAlias comparison", "[qualified_name]") {
	BindingAlias nested("c", duckdb::vector<Identifier> {"s1", "s2"}, "t");
	REQUIRE(nested == BindingAlias("c", duckdb::vector<Identifier> {"s1", "s2"}, "t"));
	REQUIRE(!(nested == BindingAlias("c", duckdb::vector<Identifier> {"s3", "s2"}, "t")));
	REQUIRE(!(nested == BindingAlias("c", "s2", "t")));
	// a less specific reference matches based on its own specificity
	REQUIRE(nested.Matches(BindingAlias("t")));
	REQUIRE(nested.Matches(BindingAlias("s2", "t")));
	REQUIRE(nested.Matches(BindingAlias("c", duckdb::vector<Identifier> {"s1", "s2"}, "t")));
	REQUIRE(!nested.Matches(BindingAlias("s1", "t")));
	REQUIRE(!nested.Matches(BindingAlias("c2", duckdb::vector<Identifier> {"s1", "s2"}, "t")));
}
