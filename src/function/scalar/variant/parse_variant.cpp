#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/function/scalar/variant_functions.hpp"
#include "duckdb/common/types/type_manager.hpp"

namespace duckdb {

static void ParseVariantFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	bool all_constant = input.AllConstant();
	input.Flatten();

	auto input_data = FlatVector::GetData<string_t>(input.data[0]);
	auto type_data = FlatVector::GetData<string_t>(input.data[1]);
	auto &context = state.GetContext();
	auto &type_manager = TypeManager::Get(context);
	auto variant_type = LogicalType::VARIANT();
	for (idx_t r = 0; r < input.size(); r++) {
		if (FlatVector::IsNull(input.data[0], r) || FlatVector::IsNull(input.data[1], r)) {
			FlatVector::SetNull(result, r, true);
			continue;
		}
		auto type = type_manager.ParseLogicalType(type_data[r].GetString(), context);
		Value str_val(input_data[r].GetString());
		auto typed_val = str_val.CastAs(context, type);
		auto variant_val = typed_val.CastAs(context, variant_type);
		result.SetValue(r, variant_val);
	}
	if (all_constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

ScalarFunction ParseVariantFun::GetFunction() {
	auto variant_type = LogicalType::VARIANT();
	return ScalarFunction("parse_variant", {LogicalType::VARCHAR, LogicalType::VARCHAR}, variant_type,
	                      ParseVariantFunction);
}

} // namespace duckdb
