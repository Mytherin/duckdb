#pragma once
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

namespace duckdb {

class SequenceOption {
public:
	explicit SequenceOption(SequenceInfo type_p) : type(type_p) {
	}
	virtual ~SequenceOption() = default;

public:
	SequenceInfo type;
};

class ValueSequenceOption : public SequenceOption {
public:
	ValueSequenceOption(SequenceInfo type, Value value_p) : SequenceOption(type), value(std::move(value_p)) {
	}

public:
	Value value;
};

class QualifiedSequenceOption : public SequenceOption {
public:
	QualifiedSequenceOption(SequenceInfo type, QualifiedName qualified_name_p)
	    : SequenceOption(type), qualified_name(std::move(qualified_name_p)) {
	}

public:
	QualifiedName qualified_name;
};

//! Builds a CreateSequenceInfo from a list of parsed sequence options, applying defaults and validating bounds.
//! Shared between CREATE SEQUENCE and GENERATED ALWAYS AS IDENTITY columns.
unique_ptr<CreateSequenceInfo>
BuildSequenceInfoFromOptions(optional<vector<pair<string, unique_ptr<SequenceOption>>>> sequence_option);

} // namespace duckdb
