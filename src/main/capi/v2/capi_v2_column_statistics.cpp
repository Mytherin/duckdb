#include "duckdb/main/capi_v2/capi_v2_internal.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"

namespace duckdb::capiv2 {

//! Narrows one of the bounds of the statistics to the given value, cast to their type
static auto CV2ColumnStatisticsSetBound(BaseStatistics &stats, const Value &value, bool is_min, const char *function)
    -> void {
	auto &type = stats.GetType();
	switch (stats.GetStatsType()) {
	case StatisticsType::NUMERIC_STATS: {
		if (value.IsNull()) {
			// a NULL bound is no bound
			is_min ? NumericStats::SetMin(stats, Value(type)) : NumericStats::SetMax(stats, Value(type));
			return;
		}
		auto bound = value.DefaultTryCastAs(type);
		if (!bound) {
			throw InvalidInputException("%s: the bound %s does not cast to %s", function, value.ToString(),
			                            type.ToString());
		}
		is_min ? NumericStats::SetMin(stats, *bound) : NumericStats::SetMax(stats, *bound);
		return;
	}
	case StatisticsType::STRING_STATS: {
		if (value.IsNull()) {
			is_min ? StringStats::SetMin(stats, string_t(), StringStatsType::NO_STATS)
			       : StringStats::SetMax(stats, string_t(), StringStatsType::NO_STATS);
			return;
		}
		auto bound = value.DefaultTryCastAs(type);
		if (!bound) {
			throw InvalidInputException("%s: the bound %s does not cast to %s", function, value.ToString(),
			                            type.ToString());
		}
		// a bound may be truncated, so it is never taken for the exact minimum or maximum
		auto &bytes = StringValue::Get(*bound);
		string_t bound_string(bytes.c_str(), UnsafeNumericCast<uint32_t>(bytes.size()));
		is_min ? StringStats::SetMin(stats, bound_string, StringStatsType::TRUNCATED_STATS)
		       : StringStats::SetMax(stats, bound_string, StringStatsType::TRUNCATED_STATS);
		return;
	}
	default:
		throw InvalidInputException("%s: statistics of type %s have no bounds", function, type.ToString());
	}
}

} // namespace duckdb::capiv2

//----------------------------------------------------------------------------------------------------------------------
// Public Functions
//----------------------------------------------------------------------------------------------------------------------

using namespace duckdb::capiv2;

DUCKDB_V2_ERROR duckdb_v2_column_statistics_create(duckdb_v2_logical_type_handle type,
                                                   duckdb_v2_column_statistics_handle *statistics,
                                                   duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(type);
	DUCKDB_CHECK_ARG(statistics);
	*statistics = nullptr;
	return WithErrorHandler(err, [&]() {
		auto type_ref = Convert(type);
		const auto &column_type = *type_ref;
		if (!column_type.IsComplete()) {
			throw duckdb::InvalidInputException("Column statistics need a fully defined concrete type");
		}
		auto result = duckdb::make_uniq<duckdb::BaseStatistics>(duckdb::BaseStatistics::CreateUnknown(column_type));
		*statistics = Convert(result.release());
	});
}

DUCKDB_V2_ERROR duckdb_v2_column_statistics_set_min(duckdb_v2_column_statistics_handle statistics,
                                                    duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		CV2ColumnStatisticsSetBound(*Convert(statistics), *Convert(value), true, "duckdb_v2_column_statistics_set_min");
	});
}

DUCKDB_V2_ERROR duckdb_v2_column_statistics_set_max(duckdb_v2_column_statistics_handle statistics,
                                                    duckdb_v2_value_handle value, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	DUCKDB_CHECK_ARG(value);
	return WithErrorHandler(err, [&]() {
		CV2ColumnStatisticsSetBound(*Convert(statistics), *Convert(value), false,
		                            "duckdb_v2_column_statistics_set_max");
	});
}

DUCKDB_V2_ERROR duckdb_v2_column_statistics_set_can_have_null(duckdb_v2_column_statistics_handle statistics,
                                                              bool can_have_null, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		Convert(statistics)
		    ->Set(can_have_null ? duckdb::StatsInfo::CAN_HAVE_NULL_VALUES : duckdb::StatsInfo::CANNOT_HAVE_NULL_VALUES);
	});
}

DUCKDB_V2_ERROR duckdb_v2_column_statistics_set_can_have_valid(duckdb_v2_column_statistics_handle statistics,
                                                               bool can_have_valid, duckdb_v2_error_info_handle *err) {
	DUCKDB_CHECK_ARG(statistics);
	return WithErrorHandler(err, [&]() {
		Convert(statistics)
		    ->Set(can_have_valid ? duckdb::StatsInfo::CAN_HAVE_VALID_VALUES
		                         : duckdb::StatsInfo::CANNOT_HAVE_VALID_VALUES);
	});
}

DUCKDB_V2_ERROR duckdb_v2_column_statistics_destroy(duckdb_v2_column_statistics_handle *statistics) {
	return WithErrorHandler(nullptr, [&]() {
		if (!statistics) {
			return;
		}
		if (*statistics) {
			delete Convert(*statistics);
			*statistics = nullptr;
		}
	});
}
