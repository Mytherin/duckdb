//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core_functions/aggregate/quantile_sort_tree.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "SkipList.h"

namespace duckdb {

//! Flattens the values of a linked list into a contiguous array for interpolation.
//! The flattened values are mutable - the interpolators partially sort them in place.
template <class INPUT_TYPE>
struct FlattenedQuantileValues {
	explicit FlattenedQuantileValues(const LinkedList &linked_list)
	    : flat(PrimitiveToLogicalType<INPUT_TYPE>(), MaxValue<idx_t>(linked_list.total_capacity, 1)) {
		ListSegmentFunctions functions;
		GetSegmentDataFunctions(functions, PrimitiveToLogicalType<INPUT_TYPE>());
		functions.BuildListVector(linked_list, flat, 0);
	}

	INPUT_TYPE *Data() {
		return FlatVector::GetDataMutable<INPUT_TYPE>(flat);
	}

	//! The flattened values - strings are materialized into the vector's heap
	Vector flat;
};

struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &aggr_input) {
		state.AddElement(input, aggr_input.input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &input_data) {
		if (source.v.total_capacity == 0) {
			return;
		}
		if (input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			// append the source linked list to the target
			if (target.v.total_capacity == 0) {
				target.v = source.v;
			} else {
				target.v.last_segment->next = source.v.first_segment;
				target.v.last_segment = source.v.last_segment;
				target.v.total_capacity += source.v.total_capacity;
			}
			return;
		}
		// we cannot absorb the source - copy the values by traversing the linked list
		ListSegmentCopy<typename STATE::InputType>(input_data.allocator, source.v, target.v);
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class INPUT_TYPE>
	static void WindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                       data_ptr_t g_state) {
		D_ASSERT(partition.inputs);

		const auto &stats = partition.stats;

		//	If frames overlap significantly, then use local skip lists.
		if (stats[0].end <= stats[1].begin) {
			//	Frames can overlap
			const auto overlap = double(stats[1].begin - stats[0].end);
			const auto cover = double(stats[1].end - stats[0].begin);
			const auto ratio = overlap / cover;
			if (ratio > .75) {
				return;
			}
		}

		//	Build the tree
		auto &state = *reinterpret_cast<STATE *>(g_state);
		auto &window_state = state.GetOrCreateWindowState();
		window_state.qst = make_uniq<QuantileSortTree>(aggr_input_data, partition);
	}

	template <class INPUT_TYPE>
	static idx_t FrameSize(QuantileIncluded<INPUT_TYPE> &included, const SubFrames &frames) {
		//	Count the number of valid values
		idx_t n = 0;
		if (included.CannotHaveNull()) {
			for (const auto &frame : frames) {
				n += frame.end - frame.start;
			}
		} else {
			//	NULLs or FILTERed values,
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					n += included(i);
				}
			}
		}

		return n;
	}
};

template <class T>
struct SkipLess {
	inline bool operator()(const T &lhi, const T &rhi) const {
		return lhi.second < rhi.second;
	}
};

template <typename INPUT_TYPE>
struct WindowQuantileState {
	// Windowed Quantile merge sort trees
	unique_ptr<QuantileSortTree> qst;

	// Windowed Quantile skip lists
	using SkipType = pair<idx_t, INPUT_TYPE>;
	using SkipListType = duckdb_skiplistlib::skip_list::HeadNode<SkipType, SkipLess<SkipType>>;
	SubFrames prevs;
	unique_ptr<SkipListType> s;
	mutable vector<SkipType> skips;

	// Windowed MAD indirection
	idx_t count;
	vector<idx_t> m;

	using IncludedType = QuantileIncluded<INPUT_TYPE>;
	using CursorType = QuantileCursor<INPUT_TYPE>;

	WindowQuantileState() : count(0) {
	}

	inline void SetCount(size_t count_p) {
		count = count_p;
		if (count >= m.size()) {
			m.resize(count);
		}
	}

	inline SkipListType &GetSkipList(bool reset = false) {
		if (reset || !s) {
			s.reset();
			s = make_uniq<SkipListType>();
		}
		return *s;
	}

	struct SkipListUpdater {
		SkipListType &skip;
		CursorType &data;
		IncludedType &included;

		inline SkipListUpdater(SkipListType &skip, CursorType &data, IncludedType &included)
		    : skip(skip), data(data), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.remove(SkipType(begin, data[begin]));
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.insert(SkipType(begin, data[begin]));
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	void UpdateSkip(CursorType &data, const SubFrames &frames, IncludedType &included) {
		//	No overlap, or no data
		if (!s || prevs.back().end <= frames.front().start || frames.back().end <= prevs.front().start) {
			auto &skip = GetSkipList(true);
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					if (included(i)) {
						skip.insert(SkipType(i, data[i]));
					}
				}
			}
		} else {
			auto &skip = GetSkipList();
			SkipListUpdater updater(skip, data, included);
			AggregateExecutor::IntersectFrames(prevs, frames, updater);
		}
	}

	bool HasTree() const {
		return qst.get();
	}

	template <typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(CursorType &data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);
		if (qst) {
			return qst->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (s) {
			// Find the position(s) needed
			try {
				QuantileInterpolator<DISCRETE> interp(q, s->size(), false);
				s->at(interp.FRN, interp.CRN - interp.FRN + 1, skips);
				array<INPUT_TYPE, 2> dest;
				dest[0] = skips[0].second;
				if (skips.size() > 1) {
					dest[1] = skips[1].second;
				} else {
					// Avoid UMA
					dest[1] = skips[0].second;
				}
				return interp.template Extract<INPUT_TYPE, RESULT_TYPE>(dest.data(), result);
			} catch (const duckdb_skiplistlib::skip_list::IndexError &idx_err) {
				throw InternalException(idx_err.message());
			}
		} else {
			throw InternalException("No accelerator for scalar QUANTILE");
		}
	}

	template <typename CHILD_TYPE, bool DISCRETE>
	void WindowList(CursorType &data, const SubFrames &frames, const idx_t n, Vector &list, const idx_t lidx,
	                const QuantileBindData &bind_data) const {
		D_ASSERT(n > 0);
		// Result is a constant LIST<CHILD_TYPE> with a fixed length
		auto ldata = FlatVector::GetDataMutable<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetChildMutable(list);
		auto rdata = FlatVector::GetDataMutable<CHILD_TYPE>(result);

		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[lentry.offset + q] = WindowScalar<CHILD_TYPE, DISCRETE>(data, frames, n, result, quantile);
		}
	}
};

struct QuantileStandardType {
	template <class T>
	static T Operation(T input, AggregateInputData &) {
		return input;
	}
};

struct QuantileStringType {
	template <class T>
	static T Operation(T input, AggregateInputData &input_data) {
		if (input.IsInlined()) {
			return input;
		}
		auto string_data = input_data.allocator.Allocate(input.GetSize());
		memcpy(string_data, input.GetData(), input.GetSize());
		return string_t(char_ptr_cast(string_data), UnsafeNumericCast<uint32_t>(input.GetSize()));
	}
};

template <typename INPUT_TYPE, class TYPE_OP>
struct QuantileState {
	using InputType = INPUT_TYPE;
	using CursorType = QuantileCursor<INPUT_TYPE>;
	using STATE_TYPE = StateListType<StateListOf<StateInputType<0>>>;

	LinkedList v;

	// Window Quantile State (only used for window execution - not exported)
	unique_ptr<WindowQuantileState<INPUT_TYPE>> window_state;
	unique_ptr<CursorType> window_cursor;

	void AddElement(INPUT_TYPE element, AggregateInputData &aggr_input) {
		ListSegmentAppendValue<INPUT_TYPE>(aggr_input.allocator, v, element);
	}

	bool HasTree() const {
		return window_state && window_state->HasTree();
	}
	WindowQuantileState<INPUT_TYPE> &GetOrCreateWindowState() {
		if (!window_state) {
			window_state = make_uniq<WindowQuantileState<INPUT_TYPE>>();
		}
		return *window_state;
	}
	WindowQuantileState<INPUT_TYPE> &GetWindowState() {
		return *window_state;
	}
	const WindowQuantileState<INPUT_TYPE> &GetWindowState() const {
		return *window_state;
	}

	CursorType &GetOrCreateWindowCursor(const WindowPartitionInput &partition) {
		if (!window_cursor) {
			window_cursor = make_uniq<CursorType>(partition);
		}
		return *window_cursor;
	}
	CursorType &GetWindowCursor() {
		return *window_cursor;
	}
	const CursorType &GetWindowCursor() const {
		return *window_cursor;
	}
};

//===--------------------------------------------------------------------===//
// State Export
//===--------------------------------------------------------------------===//
//! The quantile parameters live in the bind data - export them as part of the state type so that the function
//! can be re-bound when the exported state is used (see RebindQuantileBindData)
template <class STATE>
AggregateStateLayout QuantileGetStateType(const BoundAggregateFunction &function,
                                          optional_ptr<FunctionData> bind_data) {
	auto layout = AggregateFunction::GetStructStateLayout<STATE>(function);
	if (bind_data) {
		auto &quantile_data = bind_data->Cast<QuantileBindData>();
		vector<Value> quantiles;
		for (auto &quantile : quantile_data.quantiles) {
			quantiles.push_back(quantile.val);
		}
		vector<Value> order;
		for (auto &entry : quantile_data.order) {
			order.push_back(Value::UBIGINT(entry));
		}
		auto quantile_type = quantiles.empty() ? LogicalType::DOUBLE : quantiles[0].type();
		layout.bind_data.push_back(Value::LIST(std::move(quantile_type), std::move(quantiles)));
		layout.bind_data.push_back(Value::LIST(LogicalType::UBIGINT, std::move(order)));
		layout.bind_data.push_back(Value::BOOLEAN(quantile_data.desc));
	}
	return layout;
}

//! Reconstructs the QuantileBindData from the values exported by QuantileGetStateType
unique_ptr<FunctionData> RebindQuantileBindData(const vector<Value> &bind_data);

} // namespace duckdb
