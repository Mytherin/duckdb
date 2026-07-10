//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/identifier.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/vector.hpp"

#include <initializer_list>
#include <iosfwd>

namespace duckdb {

//! An Identifier represents a SQL identifier (e.g. a column name, table name, alias, ...).
//! Unlike a regular string, identifiers compare case-insensitively (using StringUtil::CIEquals).
//! Internally the identifier is stored as-is (preserving the original casing), but all comparisons,
//! hashing and ordering are case-insensitive.
class Identifier {
public:
	Identifier() = default;
	//! Construction from a string literal is implicit: literals in the source are identifiers by intent.
	Identifier(const char *str) : value(str) { // NOLINT: implicit conversion from literals is intentional
	}
	//! Construction from a runtime string is explicit: an Identifier carries case-insensitive semantics, so
	//! promoting a runtime string must be a deliberate choice at the call site.
	explicit Identifier(const string &str) : value(str) {
	}
	explicit Identifier(string &&str) : value(std::move(str)) {
	}

	//! Named constructors for well-known identifiers
	static Identifier DefaultSchema() {
		return Identifier(DEFAULT_SCHEMA);
	}
	static Identifier InvalidSchema() {
		return Identifier(INVALID_SCHEMA);
	}
	static Identifier InvalidCatalog() {
		return Identifier(INVALID_CATALOG);
	}
	static Identifier SystemCatalog() {
		return Identifier(SYSTEM_CATALOG);
	}
	static Identifier TempCatalog() {
		return Identifier(TEMP_CATALOG);
	}

	//! Conversion back to a string is explicit: it discards the case-insensitive semantics, so callers must opt in
	//! (use GetIdentifierName() for the raw value). Keeping this explicit is what makes the Identifier type safe.
	explicit operator const string &() const {
		return value;
	}

	//! The raw underlying string (preserving original casing)
	const string &GetIdentifierName() const {
		return value;
	}

	bool empty() const { // NOLINT: match std::string interface
		return value.empty();
	}
	void clear() { // NOLINT: match std::string interface
		value.clear();
	}
	idx_t size() const { // NOLINT: match std::string interface
		return value.size();
	}
	const char *c_str() const { // NOLINT: match std::string interface
		return value.c_str();
	}

	//! Case-insensitive hash of the identifier
	DUCKDB_API hash_t Hash() const;
	//! Case-sensitive hash of the identifier (used when identifier comparisons are configured to be case-sensitive)
	DUCKDB_API hash_t HashCaseSensitive() const;
	//! Case-sensitive equality of two identifiers
	DUCKDB_API bool EqualsCaseSensitive(const Identifier &other) const;

private:
	string value;
};

//! Equality (case-insensitive)
DUCKDB_API bool operator==(const Identifier &a, const Identifier &b);
DUCKDB_API bool operator==(const Identifier &a, const string &b);
DUCKDB_API bool operator==(const string &a, const Identifier &b);
DUCKDB_API bool operator==(const Identifier &a, const char *b);
DUCKDB_API bool operator==(const char *a, const Identifier &b);

inline bool operator!=(const Identifier &a, const Identifier &b) {
	return !(a == b);
}
inline bool operator!=(const Identifier &a, const string &b) {
	return !(a == b);
}
inline bool operator!=(const string &a, const Identifier &b) {
	return !(a == b);
}
inline bool operator!=(const Identifier &a, const char *b) {
	return !(a == b);
}
inline bool operator!=(const char *a, const Identifier &b) {
	return !(a == b);
}

//! Ordering (case-insensitive)
DUCKDB_API bool operator<(const Identifier &a, const Identifier &b);

//! Streaming an identifier writes its raw name (without quotes) - this mirrors writing the underlying string
DUCKDB_API std::ostream &operator<<(std::ostream &os, const Identifier &id);

//! String concatenation (std::operator+ is a template and cannot use the implicit conversion, so we provide our own)
inline string operator+(const Identifier &a, const string &b) {
	return a.GetIdentifierName() + b;
}
inline string operator+(const string &a, const Identifier &b) {
	return a + b.GetIdentifierName();
}
inline string operator+(const Identifier &a, const char *b) {
	return a.GetIdentifierName() + b;
}
inline string operator+(const char *a, const Identifier &b) {
	return a + b.GetIdentifierName();
}
inline string operator+(const Identifier &a, const Identifier &b) {
	return a.GetIdentifierName() + b.GetIdentifierName();
}
inline string operator+(const Identifier &a, char b) {
	return a.GetIdentifierName() + b;
}
inline string operator+(char a, const Identifier &b) {
	return a + b.GetIdentifierName();
}

//! Appending an identifier to a string appends the raw name
inline string &operator+=(string &a, const Identifier &b) {
	a += b.GetIdentifierName();
	return a;
}

//! Case-sensitivity is configurable per ClientContext. The hash/equality functors carry a runtime flag so a single
//! IdentifierMap/IdentifierSet instance compares either case-insensitively (the default) or case-sensitively.
struct IdentifierHashFunction {
	IdentifierHashFunction() : case_sensitive(false) {
	}
	explicit IdentifierHashFunction(bool case_sensitive_p) : case_sensitive(case_sensitive_p) {
	}

	uint64_t operator()(const Identifier &id) const {
		return case_sensitive ? id.HashCaseSensitive() : id.Hash();
	}

	bool case_sensitive;
};

struct IdentifierEquality {
	IdentifierEquality() : case_sensitive(false) {
	}
	explicit IdentifierEquality(bool case_sensitive_p) : case_sensitive(case_sensitive_p) {
	}

	bool operator()(const Identifier &a, const Identifier &b) const {
		return case_sensitive ? a.EqualsCaseSensitive(b) : (a == b);
	}

	bool case_sensitive;
};

struct IdentifierCompare {
	bool operator()(const Identifier &a, const Identifier &b) const {
		return a < b;
	}
};

class ClientContext;

//! Tag selecting the constructor used when no ClientContext is available to determine case-sensitivity.
//! Maps/sets constructed this way are always case-insensitive (preserving DuckDB's historical behavior).
//! This should be a last resort - prefer constructing with a ClientContext wherever one is reachable.
enum class IdentifierConstructor : uint8_t { NO_CLIENT_CONTEXT };

//! Reads the identifier case-sensitivity setting from the ClientContext.
DUCKDB_API bool IdentifierCaseSensitive(ClientContext &context);

//! A case-sensitivity-aware hash map keyed by Identifier. Construction requires either a ClientContext (which
//! determines case-sensitivity) or an explicit IdentifierConstructor::NO_CLIENT_CONTEXT tag.
template <typename T>
class IdentifierMap : public unordered_map<Identifier, T, IdentifierHashFunction, IdentifierEquality> {
public:
	using base_t = unordered_map<Identifier, T, IdentifierHashFunction, IdentifierEquality>;

	explicit IdentifierMap(ClientContext &context) : IdentifierMap(IdentifierCaseSensitive(context)) {
	}
	explicit IdentifierMap(IdentifierConstructor) : IdentifierMap(false) {
	}
	//! Construct from a compile-time literal list (always case-insensitive). Used for static, well-known maps.
	IdentifierMap(IdentifierConstructor, std::initializer_list<typename base_t::value_type> init)
	    : base_t(init, 0, IdentifierHashFunction(false), IdentifierEquality(false)) {
	}

	//! Bring in the base assignment operators (notably assignment from an initializer_list, which the implicitly
	//! declared derived assignment operators would otherwise hide). These preserve the existing case-sensitivity.
	using base_t::operator=;

private:
	explicit IdentifierMap(bool case_sensitive)
	    : base_t(0, IdentifierHashFunction(case_sensitive), IdentifierEquality(case_sensitive)) {
	}
};

//! A case-sensitivity-aware hash set of Identifiers. Construction requires either a ClientContext (which determines
//! case-sensitivity) or an explicit IdentifierConstructor::NO_CLIENT_CONTEXT tag.
class IdentifierSet : public unordered_set<Identifier, IdentifierHashFunction, IdentifierEquality> {
public:
	using base_t = unordered_set<Identifier, IdentifierHashFunction, IdentifierEquality>;

	DUCKDB_API explicit IdentifierSet(ClientContext &context);
	explicit IdentifierSet(IdentifierConstructor) : IdentifierSet(false) {
	}
	//! Construct from a compile-time literal list (always case-insensitive). Used for static, well-known sets.
	IdentifierSet(IdentifierConstructor, std::initializer_list<Identifier> init)
	    : base_t(init, 0, IdentifierHashFunction(false), IdentifierEquality(false)) {
	}

	//! Bring in the base assignment operators (notably assignment from an initializer_list, which the implicitly
	//! declared derived assignment operators would otherwise hide). These preserve the existing case-sensitivity.
	using base_t::operator=;

private:
	explicit IdentifierSet(bool case_sensitive)
	    : base_t(0, IdentifierHashFunction(case_sensitive), IdentifierEquality(case_sensitive)) {
	}
};

template <typename T>
using identifier_tree_t = map<Identifier, T, IdentifierCompare>;

//! Helper to convert a vector of identifiers to a vector of (raw) strings (for interop with string-based APIs)
inline vector<string> IdentifiersToStrings(const vector<Identifier> &identifiers) {
	vector<string> result;
	result.reserve(identifiers.size());
	for (auto &identifier : identifiers) {
		result.push_back(identifier.GetIdentifierName());
	}
	return result;
}

//! Helper to convert a vector of (raw) strings to a vector of identifiers (to be removed at the end of the rework)
inline vector<Identifier> StringsToIdentifiers(const vector<string> &strings) {
	vector<Identifier> result;
	result.reserve(strings.size());
	for (auto &str : strings) {
		result.emplace_back(str);
	}
	return result;
}

//! Identifier-aware overloads of the invalid-catalog/schema checks. These live here (rather than next to the
//! string versions in constants.hpp) because constants.hpp is a dependency of this header.
inline bool IsInvalidCatalog(const Identifier &catalog) {
	return catalog.empty();
}
inline bool IsInvalidSchema(const Identifier &schema) {
	return schema.empty();
}

} // namespace duckdb
