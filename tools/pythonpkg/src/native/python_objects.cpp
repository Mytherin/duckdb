#include "duckdb_python/python_objects.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb_python/pyconnection/pyconnection.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/types/varint.hpp"
#include "duckdb/function/to_interval.hpp"

#include "datetime.h" // Python datetime initialize #1

namespace duckdb {

PyDictionary::PyDictionary(py::object dict) {
	keys = py::list(dict.attr("keys")());
	values = py::list(dict.attr("values")());
	len = py::len(keys);
	this->dict = std::move(dict);
}

PyTimeDelta::PyTimeDelta(py::handle &obj) {
	days = PyTimeDelta::GetDays(obj);
	seconds = PyTimeDelta::GetSeconds(obj);
	microseconds = PyTimeDelta::GetMicros(obj);
}

interval_t PyTimeDelta::ToInterval() {
	interval_t result;

	auto micros_interval = Interval::FromMicro(microseconds);
	auto days_interval = interval_t {/*months = */ 0,
	                                 /*days = */ days,
	                                 /*micros = */ 0};
	auto seconds_interval = ToSecondsOperator::Operation<int64_t, interval_t>(seconds);

	result = AddOperator::Operation<interval_t, interval_t, interval_t>(micros_interval, days_interval);
	result = AddOperator::Operation<interval_t, interval_t, interval_t>(result, seconds_interval);
	return result;
}

int64_t PyTimeDelta::GetDays(py::handle &obj) {
	return py::int_(obj.attr("days")).cast<int64_t>();
}

int64_t PyTimeDelta::GetSeconds(py::handle &obj) {
	return py::int_(obj.attr("seconds")).cast<int64_t>();
}

int64_t PyTimeDelta::GetMicros(py::handle &obj) {
	return py::int_(obj.attr("microseconds")).cast<int64_t>();
}

PyDecimal::PyDecimal(py::handle &obj) : obj(obj) {
	auto as_tuple = obj.attr("as_tuple")();

	py::object exponent = as_tuple.attr("exponent");
	SetExponent(exponent);

	auto sign = py::cast<int8_t>(as_tuple.attr("sign"));
	signed_value = sign != 0;

	auto decimal_digits = as_tuple.attr("digits");
	auto width = py::len(decimal_digits);
	digits.reserve(width);
	for (auto digit : decimal_digits) {
		digits.push_back(py::cast<uint8_t>(digit));
	}
}

bool PyDecimal::TryGetType(LogicalType &type) {
	int32_t width = digits.size();

	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
	case PyDecimalExponentType::EXPONENT_POWER: {
		auto scale = exponent_value;
		if (exponent_type == PyDecimalExponentType::EXPONENT_POWER) {
			width += scale;
		}
		if (scale > width) {
			// The value starts with 1 or more zeros, which are optimized out of the 'digits' array
			// 0.001; width=1, exponent=-3
			width = scale + 1; // DECIMAL(4,3) - add 1 for the non-decimal values
		}
		if (width > Decimal::MAX_WIDTH_INT128) {
			type = LogicalType::DOUBLE;
			return true;
		}
		type = LogicalType::DECIMAL(width, scale);
		return true;
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		type = LogicalType::FLOAT;
		return true;
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		type = LogicalType::FLOAT;
		return true;
	}
	default: // LCOV_EXCL_START
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
	}
	return true;
}
// LCOV_EXCL_START
static void ExponentNotRecognized() {
	throw NotImplementedException("Failed to convert decimal.Decimal value, exponent type is unknown");
}
// LCOV_EXCL_STOP

void PyDecimal::SetExponent(py::handle &exponent) {
	if (py::isinstance<py::int_>(exponent)) {
		this->exponent_value = py::cast<int32_t>(exponent);
		if (this->exponent_value >= 0) {
			exponent_type = PyDecimalExponentType::EXPONENT_POWER;
			return;
		}
		exponent_value *= -1;
		exponent_type = PyDecimalExponentType::EXPONENT_SCALE;
		return;
	}
	if (py::isinstance<py::str>(exponent)) {
		string exponent_string = py::str(exponent);
		if (exponent_string == "n") {
			exponent_type = PyDecimalExponentType::EXPONENT_NAN;
			return;
		}
		if (exponent_string == "F") {
			exponent_type = PyDecimalExponentType::EXPONENT_INFINITY;
			return;
		}
	}
	// LCOV_EXCL_START
	ExponentNotRecognized();
	// LCOV_EXCL_STOP
}

static bool WidthFitsInDecimal(int32_t width) {
	return width >= 0 && width <= Decimal::MAX_WIDTH_DECIMAL;
}

template <class OP>
Value PyDecimalCastSwitch(PyDecimal &decimal, uint8_t width, uint8_t scale) {
	if (width > DecimalWidth<int64_t>::max) {
		return OP::template Operation<hugeint_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	if (width > DecimalWidth<int32_t>::max) {
		return OP::template Operation<int64_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	if (width > DecimalWidth<int16_t>::max) {
		return OP::template Operation<int32_t>(decimal.signed_value, decimal.digits, width, scale);
	}
	return OP::template Operation<int16_t>(decimal.signed_value, decimal.digits, width, scale);
}

// Wont fit in a DECIMAL, fall back to DOUBLE
static Value CastToDouble(py::handle &obj) {
	string converted = py::str(obj);
	string_t decimal_string(converted);
	double double_val;
	bool try_cast = TryCast::Operation<string_t, double>(decimal_string, double_val, true);
	(void)try_cast;
	D_ASSERT(try_cast);
	return Value::DOUBLE(double_val);
}

Value PyDecimal::ToDuckValue() {
	int32_t width = digits.size();
	if (!WidthFitsInDecimal(width)) {
		return CastToDouble(obj);
	}
	switch (exponent_type) {
	case PyDecimalExponentType::EXPONENT_SCALE: {
		uint8_t scale = exponent_value;
		D_ASSERT(WidthFitsInDecimal(width));
		if (scale > width) {
			// Values like '0.001'
			width = scale + 1; // leave 1 room for the non-decimal value
		}
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalScaleConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_POWER: {
		uint8_t scale = exponent_value;
		width += scale;
		if (!WidthFitsInDecimal(width)) {
			return CastToDouble(obj);
		}
		return PyDecimalCastSwitch<PyDecimalPowerConverter>(*this, width, scale);
	}
	case PyDecimalExponentType::EXPONENT_NAN: {
		return Value::FLOAT(NAN);
	}
	case PyDecimalExponentType::EXPONENT_INFINITY: {
		return Value::FLOAT(INFINITY);
	}
	// LCOV_EXCL_START
	default: {
		throw NotImplementedException("case not implemented for type PyDecimalExponentType");
	} // LCOV_EXCL_STOP
	}
}

PyTime::PyTime(py::handle &obj) : obj(obj) {
	hour = PyTime::GetHours(obj);          // NOLINT
	minute = PyTime::GetMinutes(obj);      // NOLINT
	second = PyTime::GetSeconds(obj);      // NOLINT
	microsecond = PyTime::GetMicros(obj);  // NOLINT
	timezone_obj = PyTime::GetTZInfo(obj); // NOLINT
}
dtime_t PyTime::ToDuckTime() {
	return Time::FromTime(hour, minute, second, microsecond);
}

Value PyTime::ToDuckValue() {
	auto duckdb_time = this->ToDuckTime();
	if (!py::none().is(this->timezone_obj)) {
		auto seconds = PyTimezone::GetUTCOffsetSeconds(this->timezone_obj);
		return Value::TIMETZ(dtime_tz_t(duckdb_time, seconds));
	}
	return Value::TIME(duckdb_time);
}

int32_t PyTime::GetHours(py::handle &obj) {
	return PyDateTime_TIME_GET_HOUR(obj.ptr()); // NOLINT
}

int32_t PyTime::GetMinutes(py::handle &obj) {
	return PyDateTime_TIME_GET_MINUTE(obj.ptr()); // NOLINT
}

int32_t PyTime::GetSeconds(py::handle &obj) {
	return PyDateTime_TIME_GET_SECOND(obj.ptr()); // NOLINT
}

int32_t PyTime::GetMicros(py::handle &obj) {
	return PyDateTime_TIME_GET_MICROSECOND(obj.ptr()); // NOLINT
}

py::object PyTime::GetTZInfo(py::handle &obj) {
	// The object returned is borrowed, there is no reference to steal
	return py::reinterpret_borrow<py::object>(PyDateTime_TIME_GET_TZINFO(obj.ptr())); // NOLINT
}

interval_t PyTimezone::GetUTCOffset(py::handle &datetime, py::handle &tzone_obj) {
	// The datetime object is provided because the utcoffset could be ambiguous
	auto res = tzone_obj.attr("utcoffset")(datetime);
	auto timedelta = PyTimeDelta(res);
	return timedelta.ToInterval();
}

int32_t PyTimezone::GetUTCOffsetSeconds(py::handle &tzone_obj) {
	// We should be able to use None here, the tzone_obj of a datetime.time should never be ambiguous
	auto res = tzone_obj.attr("utcoffset")(py::none());
	auto timedelta = PyTimeDelta(res);
	if (timedelta.days != 0) {
		throw InvalidInputException(
		    "Failed to convert 'tzinfo' object, utcoffset returned an invalid timedelta (days)");
	}
	if (timedelta.microseconds != 0) {
		throw InvalidInputException(
		    "Failed to convert 'tzinfo' object, utcoffset returned an invalid timedelta (microseconds)");
	}
	return timedelta.seconds;
}

PyDateTime::PyDateTime(py::handle &obj) : obj(obj) {
	year = PyDateTime::GetYears(obj);
	month = PyDateTime::GetMonths(obj);
	day = PyDateTime::GetDays(obj);
	hour = PyDateTime::GetHours(obj);
	minute = PyDateTime::GetMinutes(obj);
	second = PyDateTime::GetSeconds(obj);
	micros = PyDateTime::GetMicros(obj);
	tzone_obj = PyDateTime::GetTZInfo(obj);
}

timestamp_t PyDateTime::ToTimestamp() {
	auto date = ToDate();
	auto time = ToDuckTime();
	return Timestamp::FromDatetime(date, time);
}

Value PyDateTime::ToDuckValue(const LogicalType &target_type) {
	auto timestamp = ToTimestamp();
	if (!py::none().is(tzone_obj)) {
		auto utc_offset = PyTimezone::GetUTCOffset(obj, tzone_obj);
		// Need to subtract the UTC offset, so we invert the interval
		utc_offset = Interval::Invert(utc_offset);
		timestamp = Interval::Add(timestamp, utc_offset);
		return Value::TIMESTAMPTZ(timestamp_tz_t(timestamp));
	}
	switch (target_type.id()) {
	case LogicalTypeId::UNKNOWN:
	case LogicalTypeId::TIMESTAMP: {
		return Value::TIMESTAMP(timestamp);
	}
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		// Because the 'Time::FromTime' method constructs a regular (usecond) timestamp, this is not compatible with
		// creating sec/ms/ns timestamps
		throw NotImplementedException("Conversion from 'datetime' to type %s is not implemented yet",
		                              target_type.ToString());
	default:
		throw ConversionException("Could not convert 'datetime' to type %s", target_type.ToString());
	}
}

date_t PyDateTime::ToDate() {
	return Date::FromDate(year, month, day);
}
dtime_t PyDateTime::ToDuckTime() {
	return Time::FromTime(hour, minute, second, micros);
}

int32_t PyDateTime::GetYears(py::handle &obj) {
	return PyDateTime_GET_YEAR(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetMonths(py::handle &obj) {
	return PyDateTime_GET_MONTH(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetDays(py::handle &obj) {
	return PyDateTime_GET_DAY(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetHours(py::handle &obj) {
	return PyDateTime_DATE_GET_HOUR(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetMinutes(py::handle &obj) {
	return PyDateTime_DATE_GET_MINUTE(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetSeconds(py::handle &obj) {
	return PyDateTime_DATE_GET_SECOND(obj.ptr()); // NOLINT
}

int32_t PyDateTime::GetMicros(py::handle &obj) {
	return PyDateTime_DATE_GET_MICROSECOND(obj.ptr()); // NOLINT
}

py::object PyDateTime::GetTZInfo(py::handle &obj) {
	// The object returned is borrowed, there is no reference to steal
	return py::reinterpret_borrow<py::object>(PyDateTime_DATE_GET_TZINFO(obj.ptr())); // NOLINT
}

PyDate::PyDate(py::handle &ele) {
	year = PyDateTime::GetYears(ele);
	month = PyDateTime::GetMonths(ele);
	day = PyDateTime::GetDays(ele);
}

date_t PyDate::ToDate() {
	return Date::FromDate(year, month, day);
}

Value PyDate::ToDuckValue() {
	auto value = Value::DATE(year, month, day);
	return value;
}

void PythonObject::Initialize() {
	PyDateTime_IMPORT; // NOLINT: Python datetime initialize #2
}

enum class InfinityType : uint8_t { NONE, POSITIVE, NEGATIVE };

InfinityType GetTimestampInfinityType(timestamp_t &timestamp) {
	if (timestamp == timestamp_t::infinity()) {
		return InfinityType::POSITIVE;
	}
	if (timestamp == timestamp_t::ninfinity()) {
		return InfinityType::NEGATIVE;
	}
	return InfinityType::NONE;
}

py::object PythonObject::FromStruct(const Value &val, const LogicalType &type,
                                    const ClientProperties &client_properties) {
	auto &struct_values = StructValue::GetChildren(val);

	auto &child_types = StructType::GetChildTypes(type);
	if (StructType::IsUnnamed(type)) {
		py::tuple py_tuple(struct_values.size());
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_entry = child_types[i];
			D_ASSERT(child_entry.first.empty());
			auto &child_type = child_entry.second;
			py_tuple[i] = FromValue(struct_values[i], child_type, client_properties);
		}
		return std::move(py_tuple);
	} else {
		py::dict py_struct;
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &child_entry = child_types[i];
			auto &child_name = child_entry.first;
			auto &child_type = child_entry.second;
			py_struct[child_name.c_str()] = FromValue(struct_values[i], child_type, client_properties);
		}
		return std::move(py_struct);
	}
}

static bool KeyIsHashable(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::ENUM:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::BIT:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME:
	case LogicalTypeId::DATE:
	case LogicalTypeId::UUID:
	case LogicalTypeId::INTERVAL:
		return true;
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::UNION: {
		idx_t count = UnionType::GetMemberCount(type);
		for (idx_t i = 0; i < count; i++) {
			if (!KeyIsHashable(UnionType::GetMemberType(type, i))) {
				return false;
			}
		}
		// Only if all the member types are hashable do we say the entire UNION is hashable
		return true;
	}
	case LogicalTypeId::STRUCT:
		return false;
	default:
		throw NotImplementedException("Unsupported type: \"%s\"", type.ToString());
	}
}

py::object PythonObject::FromValue(const Value &val, const LogicalType &type,
                                   const ClientProperties &client_properties) {
	auto &import_cache = *DuckDBPyConnection::ImportCache();
	if (val.IsNull()) {
		return py::none();
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return py::cast(val.GetValue<bool>());
	case LogicalTypeId::TINYINT:
		return py::cast(val.GetValue<int8_t>());
	case LogicalTypeId::SMALLINT:
		return py::cast(val.GetValue<int16_t>());
	case LogicalTypeId::INTEGER:
		return py::cast(val.GetValue<int32_t>());
	case LogicalTypeId::BIGINT:
		return py::cast(val.GetValue<int64_t>());
	case LogicalTypeId::UTINYINT:
		return py::cast(val.GetValue<uint8_t>());
	case LogicalTypeId::USMALLINT:
		return py::cast(val.GetValue<uint16_t>());
	case LogicalTypeId::UINTEGER:
		return py::cast(val.GetValue<uint32_t>());
	case LogicalTypeId::UBIGINT:
		return py::cast(val.GetValue<uint64_t>());
	case LogicalTypeId::HUGEINT:
		return py::reinterpret_steal<py::object>(PyLong_FromString(val.GetValue<string>().c_str(), nullptr, 10));
	case LogicalTypeId::UHUGEINT:
		return py::reinterpret_steal<py::object>(PyLong_FromString(val.GetValue<string>().c_str(), nullptr, 10));
	case LogicalTypeId::FLOAT:
		return py::cast(val.GetValue<float>());
	case LogicalTypeId::DOUBLE:
		return py::cast(val.GetValue<double>());
	case LogicalTypeId::DECIMAL: {
		return import_cache.decimal.Decimal()(val.ToString());
	}
	case LogicalTypeId::ENUM:
		return py::cast(EnumType::GetValue(val));
	case LogicalTypeId::UNION: {
		return PythonObject::FromValue(UnionValue::GetValue(val), UnionValue::GetType(val), client_properties);
	}
	case LogicalTypeId::VARCHAR:
		return py::cast(StringValue::Get(val));
	case LogicalTypeId::BLOB:
		return py::bytes(StringValue::Get(val));
	case LogicalTypeId::BIT:
		return py::cast(Bit::ToString(StringValue::Get(val)));
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_TZ: {
		D_ASSERT(type.InternalType() == PhysicalType::INT64);
		auto timestamp = val.GetValueUnsafe<timestamp_t>();

		InfinityType infinity = GetTimestampInfinityType(timestamp);
		if (infinity == InfinityType::POSITIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.max());
		}
		if (infinity == InfinityType::NEGATIVE) {
			return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.min());
		}

		if (type.id() == LogicalTypeId::TIMESTAMP_MS) {
			timestamp = Timestamp::FromEpochMs(timestamp.value);
		} else if (type.id() == LogicalTypeId::TIMESTAMP_NS) {
			timestamp = Timestamp::FromEpochNanoSeconds(timestamp.value);
		} else if (type.id() == LogicalTypeId::TIMESTAMP_SEC) {
			timestamp = Timestamp::FromEpochSeconds(timestamp.value);
		}

		int32_t year, month, day, hour, min, sec, micros;
		date_t date;
		dtime_t time;
		Timestamp::Convert(timestamp, date, time);
		Date::Convert(date, year, month, day);
		Time::Convert(time, hour, min, sec, micros);
		py::object py_timestamp;
		try {
			auto python_conversion = PyDateTime_FromDateAndTime(year, month, day, hour, min, sec, micros);
			if (!python_conversion) {
				throw py::error_already_set();
			}
			py_timestamp = py::reinterpret_steal<py::object>(python_conversion);
		} catch (py::error_already_set &e) {
			// Failed to convert, fall back to str
			return py::str(val.ToString());
		}
		if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
			// We have to add the timezone info
			auto tz_utc = import_cache.pytz.timezone()("UTC");
			auto timestamp_utc = tz_utc.attr("localize")(py_timestamp);
			auto tz_info = import_cache.pytz.timezone()(client_properties.time_zone);
			return timestamp_utc.attr("astimezone")(tz_info);
		}
		return py_timestamp;
	}
	case LogicalTypeId::TIME_TZ: {
		D_ASSERT(type.InternalType() == PhysicalType::INT64);
		int32_t hour, min, sec, microsec;
		auto time_tz = val.GetValueUnsafe<dtime_tz_t>();
		auto time = time_tz.time();
		auto offset = time_tz.offset();
		duckdb::Time::Convert(time, hour, min, sec, microsec);
		py::object py_time;
		try {
			auto python_conversion = PyTime_FromTime(hour, min, sec, microsec);
			if (!python_conversion) {
				throw py::error_already_set();
			}
			py_time = py::reinterpret_steal<py::object>(python_conversion);
		} catch (py::error_already_set &e) {
			// Failed to convert, fall back to str
			return py::str(val.ToString());
		}
		// We have to add the timezone info
		auto timedelta = import_cache.datetime.timedelta()(py::arg("seconds") = offset);
		auto timezone_offset = import_cache.datetime.timezone()(timedelta);
		auto tmp_datetime = import_cache.datetime.datetime.min();
		auto tmp_datetime_with_tz = import_cache.datetime.datetime.combine()(tmp_datetime, py_time, timezone_offset);
		return tmp_datetime_with_tz.attr("timetz")();
	}
	case LogicalTypeId::TIME: {
		D_ASSERT(type.InternalType() == PhysicalType::INT64);
		int32_t hour, min, sec, microsec;
		auto time = val.GetValueUnsafe<dtime_t>();
		duckdb::Time::Convert(time, hour, min, sec, microsec);
		try {
			auto pytime = PyTime_FromTime(hour, min, sec, microsec);
			if (!pytime) {
				throw py::error_already_set();
			}
			return py::reinterpret_steal<py::object>(pytime);
		} catch (py::error_already_set &e) {
			return py::str(val.ToString());
		}
	}
	case LogicalTypeId::DATE: {
		D_ASSERT(type.InternalType() == PhysicalType::INT32);
		auto date = val.GetValueUnsafe<date_t>();
		int32_t year, month, day;
		if (!duckdb::Date::IsFinite(date)) {
			if (date == date_t::infinity()) {
				return py::reinterpret_borrow<py::object>(import_cache.datetime.date.max());
			}
			return py::reinterpret_borrow<py::object>(import_cache.datetime.date.min());
		}
		duckdb::Date::Convert(date, year, month, day);
		try {
			auto pydate = PyDate_FromDate(year, month, day);
			if (!pydate) {
				throw py::error_already_set();
			}
			return py::reinterpret_steal<py::object>(pydate);
		} catch (py::error_already_set &e) {
			return py::str(val.ToString());
		}
	}
	case LogicalTypeId::LIST: {
		auto &list_values = ListValue::GetChildren(val);

		py::list list;
		for (auto &list_elem : list_values) {
			list.append(FromValue(list_elem, ListType::GetChildType(type), client_properties));
		}
		return std::move(list);
	}
	case LogicalTypeId::ARRAY: {
		auto &array_values = ArrayValue::GetChildren(val);
		auto array_size = ArrayType::GetSize(type);
		auto &child_type = ArrayType::GetChildType(type);

		// do not remove the static cast here, it's required for building
		// duckdb-python with Emscripten.
		//
		// without this cast, a static_assert fails in pybind11
		// because the return type of ArrayType::GetSize is idx_t,
		// which is typedef'd to uint64_t and ssize_t is 4 bytes with Emscripten
		// and pybind11 requires that the input be castable to ssize_t
		py::tuple arr(static_cast<py::ssize_t>(array_size));

		for (idx_t elem_idx = 0; elem_idx < array_size; elem_idx++) {
			arr[elem_idx] = FromValue(array_values[elem_idx], child_type, client_properties);
		}
		return std::move(arr);
	}
	case LogicalTypeId::MAP: {
		auto &list_values = ListValue::GetChildren(val);

		auto &key_type = MapType::KeyType(type);
		auto &val_type = MapType::ValueType(type);

		py::dict py_struct;
		if (KeyIsHashable(key_type)) {
			for (auto &list_elem : list_values) {
				auto &struct_children = StructValue::GetChildren(list_elem);
				auto key = PythonObject::FromValue(struct_children[0], key_type, client_properties);
				auto value = PythonObject::FromValue(struct_children[1], val_type, client_properties);
				py_struct[std::move(key)] = std::move(value);
			}
		} else {
			py::list keys;
			py::list values;
			for (auto &list_elem : list_values) {
				auto &struct_children = StructValue::GetChildren(list_elem);
				keys.append(PythonObject::FromValue(struct_children[0], key_type, client_properties));
				values.append(PythonObject::FromValue(struct_children[1], val_type, client_properties));
			}
			py_struct["key"] = std::move(keys);
			py_struct["value"] = std::move(values);
		}
		return std::move(py_struct);
	}
	case LogicalTypeId::STRUCT: {
		return FromStruct(val, type, client_properties);
	}
	case LogicalTypeId::UUID: {
		auto uuid_value = val.GetValueUnsafe<hugeint_t>();
		return import_cache.uuid.UUID()(UUID::ToString(uuid_value));
	}
	case LogicalTypeId::VARINT: {
		auto varint_value = val.GetValueUnsafe<varint_t>();
		return py::str(Varint::VarIntToVarchar(varint_value));
	}
	case LogicalTypeId::INTERVAL: {
		auto interval_value = val.GetValueUnsafe<interval_t>();
		int64_t days = duckdb::Interval::DAYS_PER_MONTH * interval_value.months + interval_value.days;
		return import_cache.datetime.timedelta()(py::arg("days") = days,
		                                         py::arg("microseconds") = interval_value.micros);
	}

	default:
		throw NotImplementedException("Unsupported type: \"%s\"", type.ToString());
	}
}

} // namespace duckdb
