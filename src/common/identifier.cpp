#include "duckdb/common/identifier.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/main/client_context.hpp"

#include <ostream>

namespace duckdb {

hash_t Identifier::Hash() const {
	return StringUtil::CIHash(value);
}

hash_t Identifier::HashCaseSensitive() const {
	return duckdb::Hash(value.c_str(), value.size());
}

bool Identifier::EqualsCaseSensitive(const Identifier &other) const {
	return value == other.value;
}

bool IdentifierCaseSensitive(ClientContext &context) {
	return context.config.case_sensitive_identifiers;
}

IdentifierSet::IdentifierSet(ClientContext &context) : IdentifierSet(IdentifierCaseSensitive(context)) {
}

bool operator==(const Identifier &a, const Identifier &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b.GetIdentifierName());
}
bool operator==(const Identifier &a, const string &b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), b);
}
bool operator==(const string &a, const Identifier &b) {
	return StringUtil::CIEquals(a, b.GetIdentifierName());
}
bool operator==(const Identifier &a, const char *b) {
	return StringUtil::CIEquals(a.GetIdentifierName(), string(b));
}
bool operator==(const char *a, const Identifier &b) {
	return StringUtil::CIEquals(string(a), b.GetIdentifierName());
}

bool operator<(const Identifier &a, const Identifier &b) {
	return StringUtil::CILessThan(a.GetIdentifierName(), b.GetIdentifierName());
}

std::ostream &operator<<(std::ostream &os, const Identifier &id) {
	return os << id.GetIdentifierName();
}

} // namespace duckdb
