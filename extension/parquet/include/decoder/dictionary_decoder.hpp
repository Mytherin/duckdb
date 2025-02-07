//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/dictionary_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_rle_bp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

struct LazyDictionary {
	LazyDictionary(Allocator &allocator, idx_t compressed_size, idx_t uncompressed_size);

	shared_ptr<ResizeableBuffer> data;
	idx_t compressed_size;
	idx_t uncompressed_size;
	idx_t dictionary_size;
	string dictionary_id;
	duckdb_parquet::CompressionCodec::type codec;
};

class DictionaryDecoder {
public:
	explicit DictionaryDecoder(ColumnReader &reader);

public:
	void SetDictionary(unique_ptr<LazyDictionary> dictionary);
	void InitializeDictionary();
	void InitializePage();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

private:
	void ConvertDictToSelVec(uint32_t *offsets, const SelectionVector &rows, idx_t count);

private:
	ColumnReader &reader;
	unique_ptr<LazyDictionary> lazy_dictionary;
	ResizeableBuffer &offset_buffer;
	unique_ptr<RleBpDecoder> dict_decoder;
	SelectionVector valid_sel;
	SelectionVector dictionary_selection_vector;
	idx_t dictionary_size;
	unique_ptr<Vector> dictionary;
	string dictionary_id;
};

} // namespace duckdb
