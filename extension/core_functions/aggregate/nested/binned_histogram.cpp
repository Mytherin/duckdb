#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/map_vector.hpp"
#include "duckdb/function/scalar/nested_functions.hpp"
#include "core_functions/aggregate/nested_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/common/types/vector.hpp"
#include "core_functions/aggregate/histogram_helpers.hpp"
#include "core_functions/scalar/generic_functions.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/algorithm.hpp"

namespace duckdb {

namespace {

template <class T>
struct HistogramBinState {
	using TYPE = T;

	unsafe_vector<T> *bin_boundaries;
	unsafe_vector<idx_t> *counts;

	void Destroy() {
		if (bin_boundaries) {
			delete bin_boundaries;
			bin_boundaries = nullptr;
		}
		if (counts) {
			delete counts;
			counts = nullptr;
		}
	}

	bool IsSet() {
		return bin_boundaries;
	}

	template <class OP>
	void InitializeBins(Vector &bin_vector, idx_t pos, AggregateInputData &aggr_input) {
		bin_boundaries = new unsafe_vector<T>();
		counts = new unsafe_vector<idx_t>();
		auto bin_counts = bin_vector.Values<list_entry_t>();
		auto bin_entry = bin_counts[pos];
		if (!bin_entry.IsValid()) {
			throw BinderException("Histogram bin list cannot be NULL");
		}
		auto bin_list = bin_entry.GetValue();

		auto &bin_child = ListVector::GetChildMutable(bin_vector);
		UnifiedVectorFormat bin_child_data;
		auto extra_state = OP::CreateExtraState();
		OP::PrepareData(bin_child, extra_state, bin_child_data);

		bin_boundaries->reserve(bin_list.length);
		for (idx_t i = 0; i < bin_list.length; i++) {
			auto bin_child_idx = bin_child_data.sel->get_index(bin_list.offset + i);
			if (!bin_child_data.validity.RowIsValid(bin_child_idx)) {
				throw BinderException("Histogram bin entry cannot be NULL");
			}
			bin_boundaries->push_back(
			    OP::template ExtractValue<T>(bin_child_data, bin_list.offset + i, aggr_input.allocator));
		}
		// sort the bin boundaries
		std::sort(bin_boundaries->begin(), bin_boundaries->end());
		// ensure there are no duplicate bin boundaries
		for (idx_t i = 1; i < bin_boundaries->size(); i++) {
			if (Equals::Operation((*bin_boundaries)[i - 1], (*bin_boundaries)[i])) {
				bin_boundaries->erase_at(i);
				i--;
			}
		}

		counts->resize(bin_list.length + 1);
	}
};

struct HistogramBinFunction {
	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		state.Destroy();
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (!source.bin_boundaries) {
			// nothing to combine
			return;
		}
		if (!target.bin_boundaries) {
			// target does not have bin boundaries - copy everything over
			target.bin_boundaries = new unsafe_vector<typename STATE::TYPE>();
			target.counts = new unsafe_vector<idx_t>();
			*target.bin_boundaries = *source.bin_boundaries;
			*target.counts = *source.counts;
		} else {
			// both source and target have bin boundaries
			if (*target.bin_boundaries != *source.bin_boundaries) {
				throw NotImplementedException(
				    "Histogram - cannot combine histograms with different bin boundaries. "
				    "Bin boundaries must be the same for all histograms within the same group");
			}
			if (target.counts->size() != source.counts->size()) {
				throw InternalException("Histogram combine - bin boundaries are the same but counts are different");
			}
			D_ASSERT(target.counts->size() == source.counts->size());
			for (idx_t bin_idx = 0; bin_idx < target.counts->size(); bin_idx++) {
				(*target.counts)[bin_idx] += (*source.counts)[bin_idx];
			}
		}
	}
};

struct HistogramRange {
	static constexpr bool EXACT = false;

	template <class T>
	static idx_t GetBin(T value, const unsafe_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value);
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

struct HistogramExact {
	static constexpr bool EXACT = true;

	template <class T>
	static idx_t GetBin(T value, const unsafe_vector<T> &bin_boundaries) {
		auto entry = std::lower_bound(bin_boundaries.begin(), bin_boundaries.end(), value);
		if (entry == bin_boundaries.end() || !(*entry == value)) {
			// entry not found - return last bucket
			return bin_boundaries.size();
		}
		return UnsafeNumericCast<idx_t>(entry - bin_boundaries.begin());
	}
};

template <class OP, class T, class HIST>
void HistogramBinUpdateFunction(Vector inputs[], AggregateInputData &aggr_input, idx_t input_count,
                                Vector &state_vector, idx_t count) {
	auto &input = inputs[0];
	auto &bin_vector = inputs[1];

	auto extra_state = OP::CreateExtraState();
	UnifiedVectorFormat input_data;
	OP::PrepareData(input, extra_state, input_data);

	auto states = state_vector.Values<HistogramBinState<T> *>();
	auto data = UnifiedVectorFormat::GetData<T>(input_data);
	for (idx_t i = 0; i < count; i++) {
		auto idx = input_data.sel->get_index(i);
		if (!input_data.validity.RowIsValid(idx)) {
			continue;
		}
		auto &state = *states[i].GetValue();
		if (!state.IsSet()) {
			state.template InitializeBins<OP>(bin_vector, i, aggr_input);
		}
		auto bin_entry = HIST::template GetBin<T>(data[idx], *state.bin_boundaries);
		++(*state.counts)[bin_entry];
	}
}

bool SupportsOtherBucket(const LogicalType &type) {
	if (type.HasAlias()) {
		return false;
	}
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::LIST:
		return true;
	default:
		return false;
	}
}
Value OtherBucketValue(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_TZ:
		return Value::MaximumValue(type);
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
		return Value::Infinity(type);
	case LogicalTypeId::VARCHAR:
		return Value("");
	case LogicalTypeId::BLOB:
		return Value::BLOB("");
	case LogicalTypeId::STRUCT: {
		// for structs we can set all child members to NULL
		auto &child_types = StructType::GetChildTypes(type);
		child_list_t<Value> child_list;
		for (auto &child_type : child_types) {
			child_list.push_back(make_pair(child_type.first, Value(child_type.second)));
		}
		return Value::STRUCT(std::move(child_list));
	}
	case LogicalTypeId::LIST:
		return Value::LIST(ListType::GetChildType(type), vector<Value>());
	default:
		throw InternalException("Unsupported type for other bucket");
	}
}

void IsHistogramOtherBinFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input_type = args.data[0].GetType();
	if (!SupportsOtherBucket(input_type)) {
		result.Reference(Value::BOOLEAN(false), count_t(args.size()));
		return;
	}
	auto v = OtherBucketValue(input_type);
	Vector ref(v, count_t(args.size()));
	VectorOperations::NotDistinctFrom(args.data[0], ref, result);

	// Set NULL if input is NULL.
	UnifiedVectorFormat input_data;
	args.data[0].ToUnifiedFormat(input_data);
	if (!input_data.validity.CannotHaveNull()) {
		auto &result_validity = FlatVector::ValidityMutable(result);
		for (idx_t idx = 0; idx < args.size(); ++idx) {
			auto input_idx = input_data.sel->get_index(idx);
			if (!input_data.validity.RowIsValid(input_idx)) {
				result_validity.SetInvalid(idx);
			}
		}
	}
}

template <class OP, class T>
void HistogramBinFinalizeFunction(Vector &state_vector, AggregateInputData &, Vector &result, idx_t count,
                                  idx_t offset) {
	auto states = state_vector.Values<HistogramBinState<T> *>();

	auto &mask = FlatVector::ValidityMutable(result);
	auto old_len = ListVector::GetListSize(result);
	idx_t new_entries = 0;
	bool supports_other_bucket = SupportsOtherBucket(MapType::KeyType(result.GetType()));
	// figure out how much space we need
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		if (!state.bin_boundaries) {
			continue;
		}
		new_entries += state.bin_boundaries->size();
		if (state.counts->back() > 0 && supports_other_bucket) {
			// overflow bucket has entries
			new_entries++;
		}
	}
	// reserve space in the list vector
	ListVector::Reserve(result, old_len + new_entries);
	auto &keys = MapVector::GetKeys(result);
	auto &values = MapVector::GetValues(result);
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto count_entries = FlatVector::GetDataMutable<uint64_t>(values);

	idx_t current_offset = old_len;
	for (idx_t i = 0; i < count; i++) {
		const auto rid = i + offset;
		auto &state = *states[i].GetValue();
		if (!state.bin_boundaries) {
			mask.SetInvalid(rid);
			continue;
		}

		auto &list_entry = list_entries[rid];
		list_entry.offset = current_offset;
		for (idx_t bin_idx = 0; bin_idx < state.bin_boundaries->size(); bin_idx++) {
			OP::template HistogramFinalize<T>((*state.bin_boundaries)[bin_idx], keys, current_offset);
			count_entries[current_offset] = (*state.counts)[bin_idx];
			current_offset++;
		}
		if (state.counts->back() > 0 && supports_other_bucket) {
			// add overflow bucket ("others")
			// set bin boundary to NULL for overflow bucket
			keys.SetValue(current_offset, OtherBucketValue(keys.GetType()));
			count_entries[current_offset] = state.counts->back();
			current_offset++;
		}
		list_entry.length = current_offset - list_entry.offset;
	}
	D_ASSERT(current_offset == old_len + new_entries);
	ListVector::SetListSize(result, current_offset);
	result.Verify();
}

//===--------------------------------------------------------------------===//
// State Export
//===--------------------------------------------------------------------===//
//! The exported state is STRUCT("bin_boundaries", "counts"): the (sorted) bin boundaries and the per-bin counters.
//! The counts list has one extra entry at the end holding the count of the overflow ("other") bucket.
template <class OP, class T>
AggregateStateLayout HistogramBinGetStateType(const BoundAggregateFunction &function) {
	child_list_t<LogicalType> children;
	children.emplace_back("bin_boundaries", LogicalType::LIST(function.GetArguments()[0]));
	children.emplace_back("counts", LogicalType::LIST(LogicalType::UBIGINT));
	AggregateStateLayout layout;
	layout.type = LogicalType::STRUCT(std::move(children));
	layout.total_state_size = AlignValue<idx_t>(sizeof(HistogramBinState<T>));
	return layout;
}

template <class OP, class T>
void HistogramBinSerializeState(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                                idx_t offset) {
	D_ASSERT(offset == 0);
	auto states = state_vector.Values<HistogramBinState<T> *>();

	auto &mask = FlatVector::ValidityMutable(result);
	auto &fields = StructVector::GetEntries(result);
	auto &boundary_lists = fields[0];
	auto &count_lists = fields[1];
	auto boundary_entries = FlatVector::ScatterWriter<list_entry_t>(boundary_lists);
	auto count_entries = FlatVector::ScatterWriter<list_entry_t>(count_lists);
	idx_t total_boundaries = ListVector::GetListSize(boundary_lists);
	idx_t total_counts = ListVector::GetListSize(count_lists);
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		boundary_entries[i].offset = total_boundaries;
		count_entries[i].offset = total_counts;
		if (!state.bin_boundaries) {
			// the bins have not been initialized - export NULL
			mask.SetInvalid(i);
			boundary_entries[i].length = 0;
			count_entries[i].length = 0;
			continue;
		}
		boundary_entries[i].length = state.bin_boundaries->size();
		count_entries[i].length = state.counts->size();
		total_boundaries += state.bin_boundaries->size();
		total_counts += state.counts->size();
	}

	ListVector::Reserve(boundary_lists, total_boundaries);
	ListVector::Reserve(count_lists, total_counts);
	auto &boundary_child = ListVector::GetChildMutable(boundary_lists);
	auto count_data = FlatVector::GetDataMutable<uint64_t>(ListVector::GetChildMutable(count_lists));
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		if (!state.bin_boundaries) {
			continue;
		}
		for (idx_t bin_idx = 0; bin_idx < state.bin_boundaries->size(); bin_idx++) {
			OP::template HistogramFinalize<T>((*state.bin_boundaries)[bin_idx], boundary_child,
			                                  boundary_entries[i].offset + bin_idx);
		}
		for (idx_t bin_idx = 0; bin_idx < state.counts->size(); bin_idx++) {
			count_data[count_entries[i].offset + bin_idx] = (*state.counts)[bin_idx];
		}
	}
	ListVector::SetListSize(boundary_lists, total_boundaries);
	ListVector::SetListSize(count_lists, total_counts);
	FlatVector::SetSize(boundary_lists, count);
	FlatVector::SetSize(count_lists, count);
	FlatVector::SetSize(result, count);
}

template <class OP, class T>
void HistogramBinDeserializeState(const AggregateStateLayout &layout, const Vector &input_vec, idx_t count,
                                  data_ptr_t dest_buffer, ArenaAllocator &allocator) {
	const auto validity = input_vec.Validity();
	const auto &fields = StructVector::GetEntries(input_vec);
	auto &boundary_lists = fields[0];
	auto &count_lists = fields[1];
	auto boundary_entries = FlatVector::GetData<list_entry_t>(boundary_lists);
	auto count_entries = FlatVector::GetData<list_entry_t>(count_lists);

	// prepare the boundaries - this encodes them the same way the state stores them (e.g. as sort keys)
	auto extra_state = OP::CreateExtraState();
	UnifiedVectorFormat boundary_data;
	OP::PrepareData(ListVector::GetChild(boundary_lists), extra_state, boundary_data);
	auto count_data = FlatVector::GetData<uint64_t>(ListVector::GetChild(count_lists));

	for (idx_t i = 0; i < count; i++) {
		auto &state = *reinterpret_cast<HistogramBinState<T> *>(dest_buffer + i * layout.total_state_size);
		if (!validity.IsValid(i)) {
			// NULL input - leave the state empty
			continue;
		}
		const auto &boundary_entry = boundary_entries[i];
		const auto &count_entry = count_entries[i];
		// note that there can be more than one extra count entry - the bin boundaries are deduplicated on
		// initialization while the counts list keeps the size of the original (non-deduplicated) bin list
		if (count_entry.length < boundary_entry.length + 1) {
			throw InvalidInputException("Invalid histogram state - the counts list should have at least one more "
			                            "entry than the bin boundaries list");
		}
		state.bin_boundaries = new unsafe_vector<T>();
		state.counts = new unsafe_vector<idx_t>();
		state.bin_boundaries->reserve(boundary_entry.length);
		for (idx_t k = 0; k < boundary_entry.length; k++) {
			state.bin_boundaries->push_back(
			    OP::template ExtractValue<T>(boundary_data, boundary_entry.offset + k, allocator));
		}
		state.counts->reserve(count_entry.length);
		for (idx_t k = 0; k < count_entry.length; k++) {
			state.counts->push_back(count_data[count_entry.offset + k]);
		}
	}
}

template <class OP, class T, class HIST>
AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	using STATE_TYPE = HistogramBinState<T>;

	const char *function_name = HIST::EXACT ? "histogram_exact" : "histogram";

	auto struct_type = LogicalType::MAP(type, LogicalType::UBIGINT);
	auto fun = AggregateFunction(
	    function_name, {type, LogicalType::LIST(type)}, struct_type, AggregateFunction::StateSize<STATE_TYPE>,
	    AggregateFunction::StateInitialize<STATE_TYPE, HistogramBinFunction>, HistogramBinUpdateFunction<OP, T, HIST>,
	    AggregateFunction::StateCombine<STATE_TYPE, HistogramBinFunction>, HistogramBinFinalizeFunction<OP, T>, nullptr,
	    nullptr, AggregateFunction::StateDestroy<STATE_TYPE, HistogramBinFunction>);
	fun.SetStateExportCallbacks(HistogramBinGetStateType<OP, T>, HistogramBinSerializeState<OP, T>,
	                            HistogramBinDeserializeState<OP, T>);
	return fun;
}

template <class HIST>
AggregateFunction GetHistogramBinFunction(const LogicalType &type) {
	if (type.id() == LogicalTypeId::DECIMAL) {
		return GetHistogramBinFunction<HIST>(LogicalType::DOUBLE);
	}
	switch (type.InternalType()) {
#ifndef DUCKDB_SMALLER_BINARY
	case PhysicalType::BOOL:
		return GetHistogramBinFunction<HistogramFunctor, bool, HIST>(type);
	case PhysicalType::UINT8:
		return GetHistogramBinFunction<HistogramFunctor, uint8_t, HIST>(type);
	case PhysicalType::UINT16:
		return GetHistogramBinFunction<HistogramFunctor, uint16_t, HIST>(type);
	case PhysicalType::UINT32:
		return GetHistogramBinFunction<HistogramFunctor, uint32_t, HIST>(type);
	case PhysicalType::UINT64:
		return GetHistogramBinFunction<HistogramFunctor, uint64_t, HIST>(type);
	case PhysicalType::INT8:
		return GetHistogramBinFunction<HistogramFunctor, int8_t, HIST>(type);
	case PhysicalType::INT16:
		return GetHistogramBinFunction<HistogramFunctor, int16_t, HIST>(type);
	case PhysicalType::INT32:
		return GetHistogramBinFunction<HistogramFunctor, int32_t, HIST>(type);
	case PhysicalType::INT64:
		return GetHistogramBinFunction<HistogramFunctor, int64_t, HIST>(type);
	case PhysicalType::FLOAT:
		return GetHistogramBinFunction<HistogramFunctor, float, HIST>(type);
	case PhysicalType::DOUBLE:
		return GetHistogramBinFunction<HistogramFunctor, double, HIST>(type);
	case PhysicalType::VARCHAR:
		return GetHistogramBinFunction<HistogramStringFunctor, string_t, HIST>(type);
#endif
	default:
		return GetHistogramBinFunction<HistogramGenericFunctor, string_t, HIST>(type);
	}
}

template <class HIST>
unique_ptr<FunctionData> HistogramBinBindFunction(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	for (auto &arg : arguments) {
		if (arg->GetReturnType().id() == LogicalTypeId::UNKNOWN) {
			throw ParameterNotResolvedException();
		}
	}

	function.ReplaceImplementation(GetHistogramBinFunction<HIST>(arguments[0]->GetReturnType()));
	return nullptr;
}

} // namespace

AggregateFunction HistogramFun::BinnedHistogramFunction() {
	return AggregateFunction("histogram", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)}, LogicalTypeId::MAP,
	                         nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                         HistogramBinBindFunction<HistogramRange>, nullptr);
}

AggregateFunction HistogramExactFun::GetFunction() {
	return AggregateFunction("histogram_exact", {LogicalType::ANY, LogicalType::LIST(LogicalType::ANY)},
	                         LogicalTypeId::MAP, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                         HistogramBinBindFunction<HistogramExact>, nullptr);
}

ScalarFunction IsHistogramOtherBinFun::GetFunction() {
	return ScalarFunction("is_histogram_other_bin", {LogicalType::ANY}, LogicalType::BOOLEAN,
	                      IsHistogramOtherBinFunction);
}

} // namespace duckdb
