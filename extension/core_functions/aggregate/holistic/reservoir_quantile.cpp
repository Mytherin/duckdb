#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/reservoir_sample.hpp"
#include "core_functions/aggregate/holistic_functions.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/common/queue.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include <algorithm>
#include <stdlib.h>

namespace duckdb {

namespace {

template <typename T>
struct ReservoirQuantileState {
	T *v;
	idx_t len;
	idx_t pos;
	BaseReservoirSampling *r_samp;

	void Resize(idx_t new_len) {
		if (new_len <= len) {
			return;
		}
		T *old_v = v;
		v = (T *)realloc(v, new_len * sizeof(T));
		if (!v) {
			free(old_v);
			throw InternalException("Memory allocation failure");
		}
		len = new_len;
	}

	void ReplaceElement(T &input) {
		v[r_samp->min_weighted_entry_index] = input;
		r_samp->ReplaceElement();
	}

	void FillReservoir(idx_t sample_size, T element) {
		if (pos < sample_size) {
			v[pos++] = element;
			r_samp->InitializeReservoirWeights(pos, len);
		} else {
			D_ASSERT(r_samp->next_index_to_sample >= r_samp->num_entries_to_skip_b4_next_sample);
			if (r_samp->next_index_to_sample == r_samp->num_entries_to_skip_b4_next_sample) {
				ReplaceElement(element);
			}
		}
	}
};

struct ReservoirQuantileBindData : public FunctionData {
	ReservoirQuantileBindData() {
	}
	ReservoirQuantileBindData(double quantile_p, idx_t sample_size_p)
	    : quantiles(1, quantile_p), sample_size(sample_size_p) {
	}

	ReservoirQuantileBindData(vector<double> quantiles_p, idx_t sample_size_p)
	    : quantiles(std::move(quantiles_p)), sample_size(sample_size_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ReservoirQuantileBindData>(quantiles, sample_size);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReservoirQuantileBindData>();
		return quantiles == other.quantiles && sample_size == other.sample_size;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
	                      const BoundAggregateFunction &function) {
		auto &bind_data = bind_data_p->Cast<ReservoirQuantileBindData>();
		serializer.WriteProperty(100, "quantiles", bind_data.quantiles);
		serializer.WriteProperty(101, "sample_size", bind_data.sample_size);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, BoundAggregateFunction &function) {
		auto result = make_uniq<ReservoirQuantileBindData>();
		deserializer.ReadProperty(100, "quantiles", result->quantiles);
		deserializer.ReadProperty(101, "sample_size", result->sample_size);
		return std::move(result);
	}

	vector<double> quantiles;
	idx_t sample_size;
};

struct ReservoirQuantileOperation {
	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input) {
		auto &bind_data = unary_input.input.bind_data->template Cast<ReservoirQuantileBindData>();
		if (state.pos == 0) {
			state.Resize(bind_data.sample_size);
		}
		if (!state.r_samp) {
			state.r_samp = new BaseReservoirSampling();
		}
		D_ASSERT(state.v);
		state.FillReservoir(bind_data.sample_size, input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.pos == 0) {
			return;
		}
		if (target.pos == 0) {
			target.Resize(source.len);
		}
		if (!target.r_samp) {
			target.r_samp = new BaseReservoirSampling();
		}
		for (idx_t src_idx = 0; src_idx < source.pos; src_idx++) {
			target.FillReservoir(target.len, source.v[src_idx]);
		}
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &aggr_input_data) {
		if (state.v) {
			free(state.v);
			state.v = nullptr;
		}
		if (state.r_samp) {
			delete state.r_samp;
			state.r_samp = nullptr;
		}
	}

	static bool IgnoreNull() {
		return true;
	}
};

struct ReservoirQuantileScalarOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}
		D_ASSERT(state.v);
		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();
		auto v_t = state.v;
		D_ASSERT(bind_data.quantiles.size() == 1);
		auto offset = (idx_t)((double)(state.pos - 1) * bind_data.quantiles[0]);
		std::nth_element(v_t, v_t + offset, v_t + state.pos);
		target = v_t[offset];
	}
};

//===--------------------------------------------------------------------===//
// State Export
//===--------------------------------------------------------------------===//
//! The exported state is STRUCT("sample_size", "values"): the size of the reservoir and the sampled values.
//! The sampling weights are not exported - they are regenerated when the state is imported, mirroring how
//! Combine re-samples the values of the source state.
template <class T>
AggregateStateLayout ReservoirQuantileGetStateType(const BoundAggregateFunction &function) {
	child_list_t<LogicalType> children;
	children.emplace_back("sample_size", LogicalType::UBIGINT);
	children.emplace_back("values", LogicalType::LIST(function.GetArguments()[0]));
	AggregateStateLayout layout;
	layout.type = LogicalType::STRUCT(std::move(children));
	layout.total_state_size = AlignValue<idx_t>(sizeof(ReservoirQuantileState<T>));
	return layout;
}

//! The shape of the exported state: STRUCT("sample_size", "values")
template <class T>
using RESERVOIR_QUANTILE_EXPORT_TYPE = VectorStructType<uint64_t, VectorListType<T>>;

template <class T>
void ReservoirQuantileSerializeState(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result,
                                     idx_t count, idx_t offset) {
	D_ASSERT(offset == 0);
	using STATE = ReservoirQuantileState<T>;
	auto states = state_vector.Values<STATE *>();
	auto writer = FlatVector::Writer<RESERVOIR_QUANTILE_EXPORT_TYPE<T>>(result, count);

	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[i].GetValue();
		if (state.pos == 0) {
			// no values have been added to this state - export NULL
			writer.WriteNull();
			continue;
		}
		writer.WriteValue([&](auto &sample_size_writer, auto &values_writer) {
			sample_size_writer.WriteValue(state.len);
			idx_t value_idx = 0;
			for (auto &value_writer : values_writer.WriteList(state.pos)) {
				value_writer.WriteValue(state.v[value_idx++]);
			}
		});
	}
}

template <class T>
void ReservoirQuantileDeserializeState(const AggregateStateLayout &layout, const Vector &input_vec, idx_t count,
                                       data_ptr_t dest_buffer, ArenaAllocator &allocator) {
	using STATE = ReservoirQuantileState<T>;
	auto entries = input_vec.Values<RESERVOIR_QUANTILE_EXPORT_TYPE<T>>();
	for (idx_t i = 0; i < count; i++) {
		auto &state = *reinterpret_cast<STATE *>(dest_buffer + i * layout.total_state_size);
		const auto entry = entries[i];
		if (!entry.IsValid()) {
			// NULL input - leave the state empty
			continue;
		}
		const auto sample_size_entry = entry.template GetChildValue<0>();
		const auto values = entry.template GetChildValue<1>();
		if (!sample_size_entry.IsValid() || !values.IsValid()) {
			throw InvalidInputException("Invalid reservoir_quantile state - the state fields cannot be NULL");
		}
		state.Resize(MaxValue<idx_t>(sample_size_entry.GetValue(), values.GetListLength()));
		idx_t value_idx = 0;
		for (const auto value_entry : values.GetChildValues()) {
			if (!value_entry.IsValid()) {
				throw InvalidInputException("Invalid reservoir_quantile state - the state values cannot be NULL");
			}
			state.v[value_idx++] = value_entry.GetValue();
		}
		state.pos = value_idx;
		state.r_samp = new BaseReservoirSampling();
		state.r_samp->InitializeReservoirWeights(state.pos, state.len);
	}
}

//! Wires the state export callbacks for the physical type of the input
void WireReservoirQuantileStateExport(AggregateFunction &fun, PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<int8_t>, ReservoirQuantileSerializeState<int8_t>,
		                            ReservoirQuantileDeserializeState<int8_t>);
		break;
	case PhysicalType::INT16:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<int16_t>, ReservoirQuantileSerializeState<int16_t>,
		                            ReservoirQuantileDeserializeState<int16_t>);
		break;
	case PhysicalType::INT32:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<int32_t>, ReservoirQuantileSerializeState<int32_t>,
		                            ReservoirQuantileDeserializeState<int32_t>);
		break;
	case PhysicalType::INT64:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<int64_t>, ReservoirQuantileSerializeState<int64_t>,
		                            ReservoirQuantileDeserializeState<int64_t>);
		break;
	case PhysicalType::INT128:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<hugeint_t>,
		                            ReservoirQuantileSerializeState<hugeint_t>,
		                            ReservoirQuantileDeserializeState<hugeint_t>);
		break;
	case PhysicalType::FLOAT:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<float>, ReservoirQuantileSerializeState<float>,
		                            ReservoirQuantileDeserializeState<float>);
		break;
	case PhysicalType::DOUBLE:
		fun.SetStateExportCallbacks(ReservoirQuantileGetStateType<double>, ReservoirQuantileSerializeState<double>,
		                            ReservoirQuantileDeserializeState<double>);
		break;
	default:
		// not supported - the state cannot be exported
		break;
	}
}

AggregateFunction GetReservoirQuantileAggregateFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int8_t>, int8_t, int8_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::TINYINT,
		                                                                                     LogicalType::TINYINT);

	case PhysicalType::INT16:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int16_t>, int16_t, int16_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::SMALLINT,
		                                                                                     LogicalType::SMALLINT);

	case PhysicalType::INT32:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int32_t>, int32_t, int32_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::INTEGER,
		                                                                                     LogicalType::INTEGER);

	case PhysicalType::INT64:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<int64_t>, int64_t, int64_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::BIGINT,
		                                                                                     LogicalType::BIGINT);

	case PhysicalType::INT128:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<hugeint_t>, hugeint_t, hugeint_t,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::HUGEINT,
		                                                                                     LogicalType::HUGEINT);
	case PhysicalType::FLOAT:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<float>, float, float,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::FLOAT,
		                                                                                     LogicalType::FLOAT);
	case PhysicalType::DOUBLE:
		return AggregateFunction::UnaryAggregateDestructor<ReservoirQuantileState<double>, double, double,
		                                                   ReservoirQuantileScalarOperation>(LogicalType::DOUBLE,
		                                                                                     LogicalType::DOUBLE);
	default:
		throw InternalException("Unimplemented reservoir quantile aggregate");
	}
}

template <class CHILD_TYPE>
struct ReservoirQuantileListOperation : public ReservoirQuantileOperation {
	template <class T, class STATE>
	static void Finalize(STATE &state, T &target, AggregateFinalizeData &finalize_data) {
		if (state.pos == 0) {
			finalize_data.ReturnNull();
			return;
		}

		D_ASSERT(finalize_data.input.bind_data);
		auto &bind_data = finalize_data.input.bind_data->template Cast<ReservoirQuantileBindData>();

		auto &result = ListVector::GetChildMutable(finalize_data.result);
		auto ridx = ListVector::GetListSize(finalize_data.result);
		ListVector::Reserve(finalize_data.result, ridx + bind_data.quantiles.size());
		auto rdata = FlatVector::GetDataMutable<CHILD_TYPE>(result);

		auto v_t = state.v;
		D_ASSERT(v_t);

		auto &entry = target;
		entry.offset = ridx;
		entry.length = bind_data.quantiles.size();
		for (size_t q = 0; q < entry.length; ++q) {
			const auto &quantile = bind_data.quantiles[q];
			auto offset = (idx_t)((double)(state.pos - 1) * quantile);
			std::nth_element(v_t, v_t + offset, v_t + state.pos);
			rdata[ridx + q] = v_t[offset];
		}

		ListVector::SetListSize(finalize_data.result, entry.offset + entry.length);
	}
};

template <class STATE, class INPUT_TYPE, class RESULT_TYPE, class OP>
AggregateFunction ReservoirQuantileListAggregate(const LogicalType &input_type, const LogicalType &child_type) {
	LogicalType result_type = LogicalType::LIST(child_type);
	return AggregateFunction(
	    {input_type}, result_type, AggregateFunction::StateSize<STATE>, AggregateFunction::StateInitialize<STATE, OP>,
	    AggregateFunction::UnaryScatterUpdate<STATE, INPUT_TYPE, OP>, AggregateFunction::StateCombine<STATE, OP>,
	    AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>, FunctionNullHandling::DEFAULT_NULL_HANDLING,
	    AggregateFunction::NoClusterUpdate(), AggregateFunction::NoBind(), AggregateFunction::StateDestroy<STATE, OP>);
}

template <typename INPUT_TYPE, typename SAVE_TYPE>
AggregateFunction GetTypedReservoirQuantileListAggregateFunction(const LogicalType &type) {
	using STATE = ReservoirQuantileState<SAVE_TYPE>;
	using OP = ReservoirQuantileListOperation<INPUT_TYPE>;
	auto fun = ReservoirQuantileListAggregate<STATE, INPUT_TYPE, list_entry_t, OP>(type, type);
	return fun;
}

AggregateFunction GetReservoirQuantileListAggregateFunction(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		return GetTypedReservoirQuantileListAggregateFunction<int8_t, int8_t>(type);
	case LogicalTypeId::SMALLINT:
		return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
	case LogicalTypeId::INTEGER:
		return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
	case LogicalTypeId::BIGINT:
		return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
	case LogicalTypeId::HUGEINT:
		return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
	case LogicalTypeId::FLOAT:
		return GetTypedReservoirQuantileListAggregateFunction<float, float>(type);
	case LogicalTypeId::DOUBLE:
		return GetTypedReservoirQuantileListAggregateFunction<double, double>(type);
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			return GetTypedReservoirQuantileListAggregateFunction<int16_t, int16_t>(type);
		case PhysicalType::INT32:
			return GetTypedReservoirQuantileListAggregateFunction<int32_t, int32_t>(type);
		case PhysicalType::INT64:
			return GetTypedReservoirQuantileListAggregateFunction<int64_t, int64_t>(type);
		case PhysicalType::INT128:
			return GetTypedReservoirQuantileListAggregateFunction<hugeint_t, hugeint_t>(type);
		default:
			throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
		}
	default:
		// TODO: Add quantitative temporal types
		throw NotImplementedException("Unimplemented reservoir quantile list aggregate");
	}
}

double CheckReservoirQuantile(const Value &quantile_val) {
	if (quantile_val.IsNull()) {
		throw BinderException("RESERVOIR_QUANTILE QUANTILE parameter cannot be NULL");
	}
	auto quantile = quantile_val.GetValue<double>();
	if (quantile < 0 || quantile > 1) {
		throw BinderException("RESERVOIR_QUANTILE can only take parameters in the range [0, 1]");
	}
	return quantile;
}

unique_ptr<FunctionData> BindReservoirQuantile(BindAggregateFunctionInput &input) {
	auto &context = input.GetClientContext();
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	D_ASSERT(arguments.size() >= 2);
	if (arguments[1]->HasParameter()) {
		throw ParameterNotResolvedException();
	}
	if (!arguments[1]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant quantile parameters");
	}
	Value quantile_val = ExpressionExecutor::EvaluateScalar(context, *arguments[1]);
	vector<double> quantiles;
	if (quantile_val.type().id() != LogicalTypeId::LIST) {
		quantiles.push_back(CheckReservoirQuantile(quantile_val));
	} else {
		for (const auto &element_val : ListValue::GetChildren(quantile_val)) {
			quantiles.push_back(CheckReservoirQuantile(element_val));
		}
	}

	if (arguments.size() == 2) {
		// remove the quantile argument so we can use the unary aggregate
		if (function.GetArguments().size() == 2) {
			Function::EraseArgument(function, arguments, arguments.size() - 1);
		} else {
			arguments.pop_back();
		}
		return make_uniq<ReservoirQuantileBindData>(quantiles, 8192ULL);
	}
	if (!arguments[2]->IsFoldable()) {
		throw BinderException("RESERVOIR_QUANTILE can only take constant sample size parameters");
	}
	Value sample_size_val = ExpressionExecutor::EvaluateScalar(context, *arguments[2]);
	if (sample_size_val.IsNull()) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample cannot be NULL");
	}
	auto sample_size = sample_size_val.GetValue<int32_t>();

	if (sample_size_val.IsNull() || sample_size <= 0) {
		throw BinderException("Size of the RESERVOIR_QUANTILE sample must be bigger than 0");
	}

	// remove the quantile arguments so we can use the unary aggregate
	if (function.GetArguments().size() == arguments.size()) {
		Function::EraseArgument(function, arguments, arguments.size() - 1);
		Function::EraseArgument(function, arguments, arguments.size() - 1);
	} else {
		arguments.pop_back();
		arguments.pop_back();
	}
	return make_uniq<ReservoirQuantileBindData>(quantiles, NumericCast<idx_t>(sample_size));
}

unique_ptr<FunctionData> ReservoirQuantileDecimalDeserialize(Deserializer &deserializer,
                                                             BoundAggregateFunction &function);

unique_ptr<FunctionData> BindReservoirQuantileDecimal(BindAggregateFunctionInput &input) {
	auto &function = input.GetBoundFunction();
	auto &arguments = input.GetArguments();
	// remember the original (pre-replacement) argument types - these are used to look the function up again
	// when e.g. re-binding an exported aggregate state
	if (function.GetOriginalArguments().empty()) {
		function.GetOriginalArguments() = function.GetArguments();
	}
	auto physical_type = arguments[0]->GetReturnType().InternalType();
	auto impl = GetReservoirQuantileAggregateFunction(physical_type);
	WireReservoirQuantileStateExport(impl, physical_type);
	function.ReplaceImplementation(impl);
	auto bind_data = BindReservoirQuantile(input);
	function.SetName("reservoir_quantile");
	function.SetSerializeCallback(ReservoirQuantileBindData::Serialize);
	function.SetDeserializeCallback(ReservoirQuantileDecimalDeserialize);
	return bind_data;
}

unique_ptr<FunctionData> ReservoirQuantileDecimalDeserialize(Deserializer &deserializer,
                                                             BoundAggregateFunction &function) {
	auto bind_data = ReservoirQuantileBindData::Deserialize(deserializer, function);
	auto &return_type = deserializer.Get<const LogicalType &>();
	auto physical_type = function.GetArguments()[0].InternalType();
	auto impl = return_type.id() == LogicalTypeId::LIST
	                ? GetReservoirQuantileListAggregateFunction(ListType::GetChildType(return_type))
	                : GetReservoirQuantileAggregateFunction(physical_type);
	WireReservoirQuantileStateExport(impl, physical_type);
	function.ReplaceImplementation(impl);
	function.SetName("reservoir_quantile");
	function.SetSerializeCallback(ReservoirQuantileBindData::Serialize);
	function.SetDeserializeCallback(ReservoirQuantileDecimalDeserialize);
	return bind_data;
}

AggregateFunction GetReservoirQuantileAggregate(PhysicalType type) {
	auto fun = GetReservoirQuantileAggregateFunction(type);
	fun.SetBindCallback(BindReservoirQuantile);
	fun.SetSerializeCallback(ReservoirQuantileBindData::Serialize);
	fun.SetDeserializeCallback(ReservoirQuantileBindData::Deserialize);
	WireReservoirQuantileStateExport(fun, type);
	// temporarily push an argument so we can bind the actual quantile
	fun.GetSignature().AddParameter(LogicalType::DOUBLE);
	return fun;
}

AggregateFunction GetReservoirQuantileListAggregate(const LogicalType &type) {
	auto fun = GetReservoirQuantileListAggregateFunction(type);
	fun.SetBindCallback(BindReservoirQuantile);
	fun.SetSerializeCallback(ReservoirQuantileBindData::Serialize);
	fun.SetDeserializeCallback(ReservoirQuantileBindData::Deserialize);
	WireReservoirQuantileStateExport(fun, type.InternalType());
	// temporarily push an argument so we can bind the actual quantile
	auto list_of_double = LogicalType::LIST(LogicalType::DOUBLE);
	fun.GetSignature().AddParameter(list_of_double);
	return fun;
}

void DefineReservoirQuantile(AggregateFunctionSet &set, const LogicalType &type) {
	//	Four versions: type, scalar/list[, count]
	auto fun = GetReservoirQuantileAggregate(type.InternalType());
	set.AddFunction(fun);

	fun.GetSignature().AddParameter(LogicalType::INTEGER);
	set.AddFunction(fun);

	// List variants
	fun = GetReservoirQuantileListAggregate(type);
	set.AddFunction(fun);

	fun.GetSignature().AddParameter(LogicalType::INTEGER);
	set.AddFunction(fun);
}

void GetReservoirQuantileDecimalFunction(AggregateFunctionSet &set, const vector<LogicalType> &arguments,
                                         const LogicalType &return_value) {
	AggregateFunction fun(arguments, return_value, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr,
	                      BindReservoirQuantileDecimal);
	fun.SetSerializeCallback(ReservoirQuantileBindData::Serialize);
	fun.SetDeserializeCallback(ReservoirQuantileDecimalDeserialize);
	set.AddFunction(fun);

	fun.GetSignature().AddParameter(LogicalType::INTEGER);
	set.AddFunction(fun);
}

} // namespace

AggregateFunctionSet ReservoirQuantileFun::GetFunctions() {
	AggregateFunctionSet reservoir_quantile;

	// DECIMAL
	GetReservoirQuantileDecimalFunction(reservoir_quantile, {LogicalTypeId::DECIMAL, LogicalType::DOUBLE},
	                                    LogicalTypeId::DECIMAL);
	GetReservoirQuantileDecimalFunction(reservoir_quantile,
	                                    {LogicalTypeId::DECIMAL, LogicalType::LIST(LogicalType::DOUBLE)},
	                                    LogicalType::LIST(LogicalTypeId::DECIMAL));

	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::TINYINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::SMALLINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::INTEGER);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::BIGINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::HUGEINT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::FLOAT);
	DefineReservoirQuantile(reservoir_quantile, LogicalTypeId::DOUBLE);
	return reservoir_quantile;
}

} // namespace duckdb
