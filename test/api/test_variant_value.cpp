#include "test_helpers.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "catch.hpp"

using namespace duckdb;

TEST_CASE("Test VARIANT Value creation", "[api]") {
	// a VARIANT value holds the underlying value directly
	auto variant_val = Value::VARIANT(Value::INTEGER(42));
	REQUIRE(variant_val.type().id() == LogicalTypeId::VARIANT);
	REQUIRE(!variant_val.IsNull());
	auto &inner = VariantUtils::GetVariantValue(variant_val);
	REQUIRE(inner.type().id() == LogicalTypeId::INTEGER);
	REQUIRE(inner.GetValue<int32_t>() == 42);

	// casting a value to VARIANT wraps the value
	auto cast_val = Value::INTEGER(42).DefaultCastAs(LogicalType::VARIANT());
	REQUIRE(cast_val.type().id() == LogicalTypeId::VARIANT);
	REQUIRE(VariantUtils::GetVariantValue(cast_val) == Value::INTEGER(42));
	REQUIRE(cast_val == variant_val);

	// wrapping a VARIANT value is a no-op
	auto rewrapped = Value::VARIANT(variant_val);
	REQUIRE(VariantUtils::GetVariantValue(rewrapped).type().id() == LogicalTypeId::INTEGER);

	// wrapping a NULL value creates a NULL VARIANT
	auto null_variant = Value::VARIANT(Value(LogicalType::INTEGER));
	REQUIRE(null_variant.type().id() == LogicalTypeId::VARIANT);
	REQUIRE(null_variant.IsNull());

	// any type can be wrapped - including nested types
	auto struct_val = Value::STRUCT({{"a", Value::INTEGER(1)}, {"b", Value("hello")}});
	auto variant_struct = Value::VARIANT(struct_val);
	REQUIRE(VariantUtils::GetVariantValue(variant_struct).type().id() == LogicalTypeId::STRUCT);
}

TEST_CASE("Test VARIANT Value roundtrip through vectors", "[api]") {
	// writing a VARIANT value into a vector converts it to the variant encoding
	// reading it back decodes it again
	auto variant_val = Value::VARIANT(Value::INTEGER(42));
	Vector vec(variant_val, count_t(1));
	REQUIRE(vec.GetType().id() == LogicalTypeId::VARIANT);
	auto read_val = vec.GetValue(0);
	REQUIRE(read_val.type().id() == LogicalTypeId::VARIANT);
	REQUIRE(VariantUtils::GetVariantValue(read_val) == Value::INTEGER(42));

	// nested values roundtrip as well
	auto list_val = Value::LIST(LogicalType::VARCHAR, {Value("hello"), Value("world")});
	Vector list_vec(Value::VARIANT(list_val), count_t(1));
	auto read_list = list_vec.GetValue(0);
	REQUIRE(VariantUtils::GetVariantValue(read_list).type().id() == LogicalTypeId::LIST);

	// NULL values roundtrip
	Vector null_vec(Value(LogicalType::VARIANT()), count_t(1));
	REQUIRE(null_vec.GetValue(0).IsNull());
}

TEST_CASE("Test empty object VARIANT Value roundtrip", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// an empty OBJECT decodes into an empty STRUCT value - which must encode back into an empty OBJECT
	Value empty_struct = Value::STRUCT(child_list_t<Value> {});
	auto wrapped = Value::VARIANT(empty_struct);
	auto str_val = wrapped.CastAs(*con.context, LogicalType::VARCHAR);
	REQUIRE(!str_val.IsNull());
	REQUIRE(StringValue::Get(str_val) == "{}");

	Vector vec(wrapped, count_t(1));
	auto roundtrip = vec.GetValue(0);
	REQUIRE(!roundtrip.IsNull());
	REQUIRE(VariantUtils::GetVariantValue(roundtrip).type().id() == LogicalTypeId::STRUCT);
}

TEST_CASE("Test VARIANT Value in queries", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// fetch a VARIANT value from a query result
	auto result = con.Query("SELECT 42::VARIANT");
	REQUIRE(!result->HasError());
	auto val = result->GetValue(0, 0);
	REQUIRE(val.type().id() == LogicalTypeId::VARIANT);
	REQUIRE(VariantUtils::GetVariantValue(val) == Value::INTEGER(42));

	// use a VARIANT value as a prepared statement parameter
	auto prepared = con.Prepare("SELECT typeof($1), $1::VARCHAR");
	REQUIRE(!prepared->HasError());
	auto prepared_result = prepared->Execute(Value::VARIANT(Value::INTEGER(42)));
	REQUIRE(!prepared_result->HasError());
	auto chunk = prepared_result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->GetValue(0, 0).ToString() == "VARIANT");
	REQUIRE(chunk->GetValue(1, 0).ToString() == "42");
}
