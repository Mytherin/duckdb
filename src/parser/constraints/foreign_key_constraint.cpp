#include "duckdb/parser/constraints/foreign_key_constraint.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {

ForeignKeyConstraint::ForeignKeyConstraint(vector<string> pk_columns, vector<string> fk_columns, ForeignKeyInfo info)
    : Constraint(ConstraintType::FOREIGN_KEY), pk_columns(std::move(pk_columns)), fk_columns(std::move(fk_columns)),
      info(std::move(info)) {
}

ForeignKeyConstraint::ForeignKeyConstraint(vector<idx_t> pk_keys, vector<idx_t> fk_keys) :
		Constraint(ConstraintType::FOREIGN_KEY){
	for(auto &pk_key : pk_keys) {
		info.pk_keys.emplace_back(pk_key);
	}
	for(auto &fk_key : fk_keys) {
		info.fk_keys.emplace_back(fk_key);
	}
}

string ForeignKeyConstraint::ToString() const {
	if (info.type == ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
		string base = "FOREIGN KEY (";

		for (idx_t i = 0; i < fk_columns.size(); i++) {
			if (i > 0) {
				base += ", ";
			}
			base += KeywordHelper::WriteOptionallyQuoted(fk_columns[i]);
		}
		base += ") REFERENCES ";
		if (!info.schema.empty()) {
			base += info.schema;
			base += ".";
		}
		base += info.table;
		base += "(";

		for (idx_t i = 0; i < pk_columns.size(); i++) {
			if (i > 0) {
				base += ", ";
			}
			base += KeywordHelper::WriteOptionallyQuoted(pk_columns[i]);
		}
		base += ")";

		return base;
	}

	return "";
}

unique_ptr<Constraint> ForeignKeyConstraint::Copy() const {
	return make_uniq<ForeignKeyConstraint>(pk_columns, fk_columns, info);
}

vector<idx_t> ForeignKeyConstraint::GetPrimaryKeys() const {
	vector<idx_t> result;
	for(auto &pk_key : info.pk_keys) {
		D_ASSERT(!pk_key.HasChildren());
		result.emplace_back(pk_key.GetPrimaryIndex());
	}
	return result;
}

vector<idx_t> ForeignKeyConstraint::GetForeignKeys() const {
	vector<idx_t> result;
	for(auto &fk_key : info.fk_keys) {
		D_ASSERT(!fk_key.HasChildren());
		result.emplace_back(fk_key.GetPrimaryIndex());
	}
	return result;
}

} // namespace duckdb
