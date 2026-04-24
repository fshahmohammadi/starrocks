// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "storage/memtable.h"

#include <memory>

#include "base/time/time.h"
#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "column/raw_data_visitor.h"
#include "common/config_ingest_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "common/logging.h"
#include "exec/sorting/sorting.h"
#include "gutil/strings/substitute.h"
#include "io/io_profiler.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "runtime/load_fail_point.h"
#include "runtime/starrocks_metrics.h"
#include "storage/chunk_helper.h"
#include "storage/memtable_sink.h"
#include "storage/non_retryable_load_errors.h"
#include "storage/primary_key_encoder.h"
#include "storage/row_store_encoder.h"
#include "storage/row_store_encoder_factory.h"
#include "storage/tablet_schema.h"
#include "storage/types.h"
#include "types/logical_type_infra.h"

namespace starrocks {

// TODO(cbl): move to common space latter
static const string LOAD_OP_COLUMN = "__op";

#define ADD_COUNTER_RELAXED(counter, value) counter.fetch_add(value, std::memory_order_relaxed)

Schema MemTable::convert_schema(
        const TabletSchemaCSPtr& tablet_schema, const std::vector<SlotDescriptor*>* slot_descs,
        const phmap::flat_hash_map<std::string, std::vector<Slice>>* passthrough_source_dicts) {
    Schema schema;
    if (tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        const auto& last_column = tablet_schema->columns().back();
        // remove last __row column if exists, because it's not used in memtable
        int ncolumn = tablet_schema->num_columns();
        if (last_column.name() == Schema::FULL_ROW_COLUMN) {
            ncolumn--;
        }
        vector<ColumnId> column_idxes;
        column_idxes.reserve(ncolumn);
        for (ColumnId i = 0; i < ncolumn; i++) {
            column_idxes.push_back(i);
        }
        schema = Schema(tablet_schema->schema(), column_idxes, tablet_schema->schema()->sort_key_idxes());
        if (slot_descs != nullptr && slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
            // load slots have __op field, so add to _vectorized_schema
            auto op_column =
                    std::make_shared<starrocks::Field>((ColumnId)-1, LOAD_OP_COLUMN, LogicalType::TYPE_TINYINT, false);
            op_column->set_aggregate_method(STORAGE_AGGREGATE_REPLACE);
            schema.append(op_column);
        }
    } else {
        schema = ChunkHelper::convert_schema(tablet_schema);
    }

    // Dict passthrough: change passthrough string fields to INT dict-code fields so that
    // _chunk and ChunkAggregator use Int32Column, keeping INT dict codes through
    // to the ColumnWriter without decoding.
    // For scalar VARCHAR -> TYPE_INT; for ARRAY<VARCHAR> -> ARRAY<INT> (change sub-field type).
    if (passthrough_source_dicts != nullptr && !passthrough_source_dicts->empty() && slot_descs != nullptr) {
        auto int_type = get_type_info(TYPE_INT);
        for (int i = 0; i < static_cast<int>(slot_descs->size()); i++) {
            const auto& slot = (*slot_descs)[i];
            if (passthrough_source_dicts->count(slot->col_name())) {
                auto original_type = schema.field(i)->type()->type();
                if (original_type == TYPE_ARRAY) {
                    // ARRAY<VARCHAR> -> ARRAY<INT>: copy the field and change the sub-field type
                    auto new_field = schema.field(i)->copy();
                    if (new_field->has_sub_fields() && !new_field->sub_fields().empty()) {
                        new_field->sub_fields()[0] = Field(new_field->sub_fields()[0].id(),
                                                           new_field->sub_fields()[0].name(),
                                                           int_type, new_field->sub_fields()[0].is_nullable());
                    }
                    schema.set_field_by_name(new_field, slot->col_name());
                } else {
                    // Scalar VARCHAR -> TYPE_INT
                    auto new_field = schema.field(i)->with_type(int_type);
                    schema.set_field_by_name(new_field, slot->col_name());
                }
            }
        }
    }

    return schema;
}

Status MemTable::prepare(PrimaryKeyEncodingType pk_encoding_type) {
    if (_keys_type != KeysType::DUP_KEYS) {
        // The ChunkAggregator used by MemTable may be used to aggregate into a large Chunk,
        // which is not suitable for obtaining Chunk from ColumnPool,
        // otherwise it will take up a lot of memory and may not be released.
        ASSIGN_OR_RETURN(_aggregator, ChunkAggregator::create(_vectorized_schema, 0, INT_MAX, 0));
    }
    if (_keys_type == KeysType::PRIMARY_KEYS) {
        if (pk_encoding_type == PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE) {
            return Status::InternalError("invalid primary key encoding type");
        }
        _pk_encoding_type = pk_encoding_type;
    }
    return Status::OK();
}

MemTable::MemTable(int64_t tablet_id, const Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
                   MemTableSink* sink, std::string merge_condition, MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _slot_descs(slot_descs),
          _keys_type(schema->keys_type()),
          _sink(sink),
          _aggregator(nullptr),
          _merge_condition(std::move(merge_condition)),
          _max_buffer_size(config::write_buffer_size),
          _mem_tracker(mem_tracker) {
    if (_keys_type == KeysType::PRIMARY_KEYS && _slot_descs != nullptr &&
        _slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        _has_op_slot = true;
    }
}

MemTable::MemTable(int64_t tablet_id, const Schema* schema, const std::vector<SlotDescriptor*>* slot_descs,
                   MemTableSink* sink, MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _slot_descs(slot_descs),
          _keys_type(schema->keys_type()),
          _sink(sink),
          _aggregator(nullptr),
          _max_buffer_size(config::write_buffer_size),
          _mem_tracker(mem_tracker) {
    if (_keys_type == KeysType::PRIMARY_KEYS && _slot_descs != nullptr &&
        _slot_descs->back()->col_name() == LOAD_OP_COLUMN) {
        _has_op_slot = true;
    }
}

MemTable::MemTable(int64_t tablet_id, const Schema* schema, MemTableSink* sink, int64_t max_buffer_size,
                   MemTracker* mem_tracker)
        : _tablet_id(tablet_id),
          _vectorized_schema(schema),
          _slot_descs(nullptr),
          _keys_type(schema->keys_type()),
          _sink(sink),
          _aggregator(nullptr),
          _max_buffer_size(max_buffer_size),
          _mem_tracker(mem_tracker) {}

MemTable::~MemTable() = default;

void MemTable::set_dict_passthrough_reverse_dicts(
        const phmap::flat_hash_map<std::string, std::vector<Slice>>* passthrough_source_dicts) {
    if (passthrough_source_dicts == nullptr || _slot_descs == nullptr) {
        return;
    }
    for (int i = 0; i < static_cast<int>(_slot_descs->size()); i++) {
        const auto& slot = (*_slot_descs)[i];
        auto it = passthrough_source_dicts->find(slot->col_name());
        if (it != passthrough_source_dicts->end()) {
            _passthrough_reverse_dicts[i] = it->second;
        }
    }
}

size_t MemTable::memory_usage() const {
    size_t size = 0;

    // used for sort
    size += sizeof(PermutationItem) * _permutations.size();
    size += sizeof(uint32_t) * _selective_values.size();

    // _result_chunk is the final result before flush
    if (_result_chunk != nullptr && _result_chunk->num_rows() > 0) {
        size += _result_chunk->memory_usage();
    }

    // _aggregator_memory_usage is 0 if keys type is DUP_KEYS
    return size + _chunk_memory_usage + _aggregator_memory_usage;
}

size_t MemTable::write_buffer_size() const {
    if (_chunk == nullptr) {
        return 0;
    }

    // _aggregator_bytes_usage is 0 if keys type is DUP_KEYS
    return _chunk_bytes_usage + _aggregator_bytes_usage;
}

size_t MemTable::write_buffer_rows() const {
    return _total_rows - _merged_rows;
}

bool MemTable::is_full() const {
    return write_buffer_size() >= _max_buffer_size || write_buffer_rows() >= _max_buffer_row;
}

bool MemTable::check_supported_column_partial_update(const Chunk& chunk) {
    return _vectorized_schema->field_names().back() != Schema::FULL_ROW_COLUMN ||
           chunk.num_columns() == _vectorized_schema->num_fields() - 1;
}

StatusOr<bool> MemTable::insert(const Chunk& chunk, const uint32_t* indexes, uint32_t from, uint32_t size) {
    auto start_time = MonotonicMicros();
    DeferOp defer([&]() { ADD_COUNTER_RELAXED(_stats.insert_time_ns, MonotonicMicros() - start_time); });
    ADD_COUNTER_RELAXED(_stats.insert_count, 1);
    if (_chunk == nullptr) {
        _chunk = ChunkHelper::new_chunk(*_vectorized_schema, 0);
    }

    bool is_column_with_row = false;
    auto full_row_col = BinaryColumn::create();
    if (_keys_type == PRIMARY_KEYS) {
        std::unique_ptr<Schema> schema_without_full_row_column;
        if (_vectorized_schema->field_names().back() == Schema::FULL_ROW_COLUMN) {
            DCHECK_GE(chunk.num_columns(), _vectorized_schema->num_fields() - 1);
            std::vector<ColumnId> cids(_vectorized_schema->num_fields() - 1);
            for (int i = 0; i < _vectorized_schema->num_fields() - 1; i++) {
                cids[i] = i;
            }
            schema_without_full_row_column = std::make_unique<Schema>(const_cast<Schema*>(_vectorized_schema), cids);
            is_column_with_row = true;
            // add row column
            auto row_encoder = RowStoreEncoderFactory::instance()->get_or_create_encoder(SIMPLE);
            if (_passthrough_reverse_dicts.empty()) {
                (void)row_encoder->encode_chunk_to_full_row_column(*schema_without_full_row_column, chunk,
                                                                   full_row_col.get());
            } else {
                // Dict passthrough: some value columns contain INT codes that must be decoded
                // to strings before encoding into the full row column.
                // Build a Columns vector with passthrough columns decoded.
                // encode_columns_to_full_row_column takes value columns only (key columns excluded).
                size_t num_key_fields = schema_without_full_row_column->num_key_fields();
                Columns value_columns;
                for (size_t i = num_key_fields; i < chunk.num_columns(); i++) {
                    auto pt_it = _passthrough_reverse_dicts.find(static_cast<int>(i));
                    if (pt_it == _passthrough_reverse_dicts.end()) {
                        value_columns.emplace_back(chunk.get_column_by_index(i));
                        continue;
                    }
                    // Decode INT codes → string using 1-based source dict vector
                    auto& source_dict = pt_it->second;
                    const auto* src_col = chunk.get_column_by_index(i).get();
                    bool is_nullable = src_col->is_nullable();

                    // Check if this is an ARRAY column (original tablet schema type)
                    if (_tablet_schema->column(i).type() == LogicalType::TYPE_ARRAY) {
                        // ARRAY<INT> passthrough -> ARRAY<VARCHAR>: decode element codes
                        const ArrayColumn* array_col;
                        const NullColumn* null_col = nullptr;
                        if (is_nullable) {
                            auto* nullable = down_cast<const NullableColumn*>(src_col);
                            array_col = down_cast<const ArrayColumn*>(nullable->data_column().get());
                            null_col = nullable->null_column().get();
                        } else {
                            array_col = down_cast<const ArrayColumn*>(src_col);
                        }
                        // Decode all elements (elements may be nullable)
                        const auto& elements = array_col->elements();
                        const Int32Column* elem_codes;
                        const NullColumn* elem_null_col = nullptr;
                        bool elements_nullable = elements.is_nullable();
                        if (elements_nullable) {
                            auto* elem_nullable = down_cast<const NullableColumn*>(&elements);
                            elem_codes = down_cast<const Int32Column*>(elem_nullable->data_column().get());
                            elem_null_col = elem_nullable->null_column().get();
                        } else {
                            elem_codes = down_cast<const Int32Column*>(&elements);
                        }
                        auto decoded_elements = BinaryColumn::create();
                        decoded_elements->reserve(elem_codes->size());
                        for (size_t r = 0; r < elem_codes->size(); r++) {
                            int32_t code = elem_codes->get_data()[r];
                            DCHECK(code >= 0 && code < source_dict.size());
                            decoded_elements->append(source_dict[code]);
                        }
                        ColumnPtr final_elements;
                        if (elements_nullable) {
                            final_elements = NullableColumn::create(decoded_elements, elem_null_col->clone());
                        } else {
                            final_elements = decoded_elements;
                        }
                        // Rebuild ArrayColumn with decoded elements and same offsets
                        auto decoded_array = ArrayColumn::create(
                                final_elements,
                                array_col->offsets_column());
                        if (is_nullable) {
                            value_columns.emplace_back(NullableColumn::create(decoded_array, null_col->clone()));
                        } else {
                            value_columns.emplace_back(decoded_array);
                        }
                    } else {
                        // Scalar INT passthrough -> VARCHAR: decode codes
                        const Int32Column* codes_col;
                        const NullColumn* null_col = nullptr;
                        if (is_nullable) {
                            auto* nullable = down_cast<const NullableColumn*>(src_col);
                            codes_col = down_cast<const Int32Column*>(nullable->data_column().get());
                            null_col = nullable->null_column().get();
                        } else {
                            codes_col = down_cast<const Int32Column*>(src_col);
                        }
                        auto decoded = BinaryColumn::create();
                        decoded->reserve(codes_col->size());
                        for (size_t r = 0; r < codes_col->size(); r++) {
                            int32_t code = codes_col->get_data()[r];
                            DCHECK(code >= 0 && code < source_dict.size());
                            decoded->append(source_dict[code]);
                        }
                        if (is_nullable) {
                            value_columns.emplace_back(NullableColumn::create(decoded, null_col->clone()));
                        } else {
                            value_columns.emplace_back(decoded);
                        }
                    }
                }
                (void)row_encoder->encode_columns_to_full_row_column(*schema_without_full_row_column,
                                                                     value_columns, *full_row_col.get());
            }
        } else {
            // when doing schema change, the chunk has shadow columns,
            // so the columns in the chunk will be more than the fields in the schema.
            DCHECK_GE(chunk.num_columns(), _vectorized_schema->num_fields());
        }
    }

    size_t cur_row_count = _chunk->num_rows();
    if (_slot_descs != nullptr) {
        // For schema change, FE will construct a shadow column.
        // The shadow column is not exist in _vectorized_schema
        // So the chunk can only be accessed by the subscript
        // instead of the column name.
        for (int i = 0; i < _slot_descs->size(); ++i) {
            const ColumnPtr& src = chunk.get_column_by_slot_id((*_slot_descs)[i]->id());
            auto* dest = _chunk->get_column_raw_ptr_by_index(i);
            dest->append_selective(*src, indexes, from, size);
        }
        if (is_column_with_row) {
            auto dest = _chunk->get_column_raw_ptr_by_name(Schema::FULL_ROW_COLUMN);
            dest->append(*full_row_col.get());
        }
    } else {
        for (int i = 0; i < _vectorized_schema->num_fields(); i++) {
            const ColumnPtr& src = chunk.get_column_by_index(i);
            auto* dest = _chunk->get_column_raw_ptr_by_index(i);
            dest->append_selective(*src, indexes, from, size);
            if (is_column_with_row && i == _vectorized_schema->num_fields() - 1) {
                dest->append(*full_row_col.get());
            }
        }
    }

    if (chunk.has_rows()) {
        _chunk_memory_usage += chunk.memory_usage() * size / chunk.num_rows();
        _chunk_bytes_usage += _chunk->bytes_usage(cur_row_count, size);
        _total_rows += chunk.num_rows();
    }

    // if memtable is full, push it to the flush executor,
    // and create a new memtable for incoming data
    bool suggest_flush = false;
    // When parallel memtable finalize is enabled, skip the early merge optimization here.
    // The merge will be done during finalize() in the flush thread instead.
    // This avoids redundant merge operations and allows the write thread to return
    // earlier, improving overall throughput by parallelizing write and finalize operations.
    if (is_full() && !config::enable_parallel_memtable_finalize) {
        size_t orig_bytes = write_buffer_size();
        RETURN_IF_ERROR(_merge());
        size_t new_bytes = write_buffer_size();
        if (new_bytes > orig_bytes * 2 / 3 && _merge_count <= 1) {
            // this means aggregate doesn't remove enough duplicate rows,
            // keep inserting into the buffer will cause additional sort&merge,
            // the cost of extra sort&merge is greater than extra flush IO,
            // so flush is suggested even buffer is not full
            suggest_flush = true;
        }
    }
    if (is_full()) {
        suggest_flush = true;
    }

    return suggest_flush;
}

Status MemTable::finalize() {
    if (_chunk == nullptr) {
        return Status::OK();
    }

    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);

        if (_keys_type != KeysType::DUP_KEYS) {
            if (_chunk->num_rows() > 0) {
                // merge last undo merge
                RETURN_IF_ERROR(_merge());
            }

            if (_merge_count > 1) {
                _chunk = _aggregator->aggregate_result();
                _aggregator->aggregate_reset();

                int64_t t1 = MonotonicMicros();
                RETURN_IF_ERROR(_sort(true));
                int64_t t2 = MonotonicMicros();
                _aggregate(true);
                int64_t t3 = MonotonicMicros();
                VLOG(2) << strings::Substitute("memtable final sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
            } else {
                // if there is only one data chunk and merge once,
                // no need to perform an additional merge.
                _chunk.reset();
                _result_chunk.reset();
            }
            _chunk_memory_usage = 0;
            _chunk_bytes_usage = 0;

            _result_chunk = _aggregator->aggregate_result();
            if (_keys_type == PRIMARY_KEYS &&
                PrimaryKeyEncoder::encode_exceed_limit(*_vectorized_schema, *_result_chunk.get(), 0,
                                                       _result_chunk->num_rows(), config::primary_key_limit_size,
                                                       _pk_encoding_type)) {
                _aggregator.reset();
                _aggregator_memory_usage = 0;
                _aggregator_bytes_usage = 0;
                return Status::Cancelled(kPrimaryKeySizeExceedError);
            }
            if (_has_op_slot) {
                // TODO(cbl): mem_tracker
                ChunkPtr upserts;
                RETURN_IF_ERROR(_split_upserts_deletes(_result_chunk, &upserts, &_deletes));
                if (_result_chunk != upserts) {
                    _result_chunk = upserts;
                }
            }
            if (_keys_type == KeysType::PRIMARY_KEYS) {
                std::vector<ColumnId> primary_key_idxes(_vectorized_schema->num_key_fields());
                for (ColumnId i = 0; i < _vectorized_schema->num_key_fields(); ++i) {
                    primary_key_idxes[i] = i;
                }
                const auto& sort_key_idxes = _vectorized_schema->sort_key_idxes();
                // if sort key columns are different with key columns, resort.
                if (std::mismatch(sort_key_idxes.begin(), sort_key_idxes.end(), primary_key_idxes.begin(),
                                  primary_key_idxes.end())
                            .first != sort_key_idxes.end()) {
                    _chunk = _result_chunk;
                    RETURN_IF_ERROR(_sort(true, true));
                }
            }
            _aggregator.reset();
            _aggregator_memory_usage = 0;
            _aggregator_bytes_usage = 0;
        } else {
            RETURN_IF_ERROR(_sort(true));
        }
    }
    // Release the input chunk after finalize to free memory earlier.
    // The finalized data is now in _result_chunk which will be used for flush.
    // This is especially important when parallel finalize is enabled, as it allows
    // the memory to be reclaimed before the flush I/O completes.
    _chunk.reset();

    ADD_COUNTER_RELAXED(_stats.finalize_time_ns, duration_ns);
    StarRocksMetrics::instance()->memtable_finalize_task_total.increment(1);
    StarRocksMetrics::instance()->memtable_finalize_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

// Flush the memtable data through the configured sink
// @param slot_idx: slot index from flush token, passed through to the sink to maintain
//                  flush order when parallel flush is enabled
Status MemTable::flush(SegmentPB* seg_info, bool eos, int64_t* flush_data_size, int64_t slot_idx) {
    FAIL_POINT_TRIGGER_EXECUTE(load_memtable_flush, MEMTABLE_FLUSH_FP_ACTION(_sink->txn_id(), _sink->tablet_id()));
    if (UNLIKELY(_result_chunk == nullptr)) {
        return Status::OK();
    }
    if (auto st = _result_chunk->capacity_limit_reached(); !st.ok()) {
        return Status::InternalError(fmt::format("memtable of tablet {} reache the capacity limit, detail msg: {}",
                                                 _tablet_id, st.message()));
    }
    auto scope = IOProfiler::scope(IOProfiler::TAG_LOAD, _tablet_id);
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        // Pass slot_idx to sink for ordering in parallel flush scenarios
        if (_deletes) {
            RETURN_IF_ERROR(_sink->flush_chunk_with_deletes(*_result_chunk, *_deletes, seg_info, eos, flush_data_size,
                                                            slot_idx));
        } else {
            RETURN_IF_ERROR(_sink->flush_chunk(*_result_chunk, seg_info, eos, flush_data_size, slot_idx));
        }
    }
    auto io_stat = scope.current_scoped_tls_io();
    ADD_COUNTER_RELAXED(_stats.flush_time_ns, duration_ns);
    ADD_COUNTER_RELAXED(_stats.io_time_ns, io_stat.write_time_ns + io_stat.sync_time_ns);
    ADD_COUNTER_RELAXED(_stats.flush_memory_size, memory_usage());
    ADD_COUNTER_RELAXED(_stats.flush_disk_size, io_stat.write_bytes);

    StarRocksMetrics::instance()->memtable_flush_total.increment(1);
    StarRocksMetrics::instance()->memtable_flush_duration_us.increment(_stats.flush_time_ns / 1000);
    StarRocksMetrics::instance()->memtable_flush_io_time_us.increment(_stats.io_time_ns / 1000);
    StarRocksMetrics::instance()->memtable_flush_memory_bytes_total.increment(_stats.flush_memory_size);
    StarRocksMetrics::instance()->memtable_flush_disk_bytes_total.increment(_stats.flush_disk_size);
    VLOG(2) << "memtable of tablet " << _tablet_id << " flush duration: " << _stats.flush_time_ns / 1000 << "us, "
            << "io time: " << _stats.io_time_ns / 1000 << "us, memory bytes: " << _stats.flush_memory_size
            << ", disk bytes: " << _stats.flush_disk_size;
    return Status::OK();
}

Status MemTable::_merge() {
    if (_chunk == nullptr || _keys_type == KeysType::DUP_KEYS) {
        return Status::OK();
    }

    int64_t t1 = MonotonicMicros();
    RETURN_IF_ERROR(_sort(false));
    int64_t t2 = MonotonicMicros();
    _aggregate(false);
    int64_t t3 = MonotonicMicros();
    VLOG(2) << strings::Substitute("memtable sort:$0 agg:$1 total:$2", t2 - t1, t3 - t2, t3 - t1);
    ++_merge_count;
    return Status::OK();
}

void MemTable::_aggregate(bool is_final) {
    if (_result_chunk == nullptr || _result_chunk->num_rows() <= 0) {
        return;
    }
    auto start_time = MonotonicNanos();
    DeferOp defer([&]() { ADD_COUNTER_RELAXED(_stats.agg_time_ns, MonotonicNanos() - start_time); });
    ADD_COUNTER_RELAXED(_stats.agg_count, 1);
    DCHECK(_result_chunk->num_rows() < INT_MAX);
    DCHECK(_aggregator->source_exhausted());

    _aggregator->update_source(_result_chunk);

    DCHECK(_aggregator->is_do_aggregate());

    _aggregator->aggregate();
    _aggregator_memory_usage = _aggregator->memory_usage();
    _aggregator_bytes_usage = _aggregator->bytes_usage();

    // impossible finish
    DCHECK(!_aggregator->is_finish());
    DCHECK(_aggregator->source_exhausted());
    _merged_rows = _aggregator->merged_rows();

    if (is_final) {
        _result_chunk.reset();
    } else {
        _result_chunk->reset();
    }
}

Status MemTable::_sort(bool is_final, bool by_sort_key) {
    auto start_time = MonotonicNanos();
    DeferOp defer([&]() { ADD_COUNTER_RELAXED(_stats.sort_time_ns, MonotonicNanos() - start_time); });
    ADD_COUNTER_RELAXED(_stats.sort_count, 1);
    SmallPermutation perm = create_small_permutation(static_cast<uint32_t>(_chunk->num_rows()));
    std::swap(perm, _permutations);

    // sort key column has some limitation right now:
    // 1. DUPLICATE TABLE and PRIMARY TABLE: no limitation
    // 2. AGGREGATE TABLE and UNIQUE TABLE: sort key columns must inclue all key columns and can not
    //    have any other columns.
    // For non-pk tables, we always sort data according to the sort key columns, as this does not affect the
    // results of the aggregation.
    // For PK tables, we need to first sort by primary key columns and remove duplicate rows, and then re-sort
    // according to the sort key columns.
    if (_keys_type != KeysType::PRIMARY_KEYS) {
        by_sort_key = true;
    }
    RETURN_IF_ERROR(_sort_column_inc(by_sort_key));
    if (is_final) {
        // No need to reserve, it will be reserve in IColumn::append_selective(),
        // Otherwise it will use more peak memory
        _result_chunk = _chunk->clone_empty_with_schema(0);
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), true);
        _chunk.reset();
    } else {
        _result_chunk = _chunk->clone_empty_with_schema();
        _append_to_sorted_chunk(_chunk.get(), _result_chunk.get(), false);
        _chunk->reset();
    }
    _chunk_memory_usage = 0;
    _chunk_bytes_usage = 0;
    return Status::OK();
}

void MemTable::_append_to_sorted_chunk(Chunk* src, Chunk* dest, bool is_final) {
    DCHECK_EQ(src->num_rows(), _permutations.size());
    permutate_to_selective(_permutations, &_selective_values);
    if (is_final) {
        dest->rolling_append_selective(*src, _selective_values.data(), 0, src->num_rows());
    } else {
        dest->append_selective(*src, _selective_values.data(), 0, src->num_rows());
    }
}

Status MemTable::_split_upserts_deletes(ChunkPtr& src, ChunkPtr* upserts, MutableColumnPtr* deletes) {
    size_t op_column_id = src->num_columns() - 1;
    auto op_column = src->get_column_by_index(op_column_id);
    src->remove_column_by_index(op_column_id);
    size_t nrows = src->num_rows();
    RawDataVisitor visitor;
    RETURN_IF_ERROR(op_column->accept(&visitor));
    const auto* ops = visitor.result();
    size_t ndel = 0;
    for (size_t i = 0; i < nrows; i++) {
        ndel += (ops[i] == TOpType::DELETE);
    }
    size_t nupsert = nrows - ndel;
    if (ndel == 0) {
        // no deletes, short path
        *upserts = src;
        return Status::OK();
    }
    if (!_merge_condition.empty()) {
        // Do not support delete with condition now
        return Status::InternalError(
                fmt::format("memtable of tablet {} delete with condition column {}", _tablet_id, _merge_condition));
    }
    vector<uint32_t> indexes[2];
    indexes[TOpType::UPSERT].reserve(nupsert);
    indexes[TOpType::DELETE].reserve(ndel);
    for (uint32_t i = 0; i < nrows; i++) {
        // ops == 0: upsert  otherwise: delete
        indexes[ops[i] == TOpType::UPSERT ? TOpType::UPSERT : TOpType::DELETE].push_back(i);
    }
    *upserts = src->clone_empty_with_schema(nupsert);
    (*upserts)->append_selective(*src, indexes[TOpType::UPSERT].data(), 0, nupsert);
    if (!(*deletes)) {
        auto st = PrimaryKeyEncoder::create_column(*_vectorized_schema, deletes, _pk_encoding_type);
        if (!st.ok()) {
            LOG(ERROR) << "create column for primary key encoder failed, schema:" << *_vectorized_schema
                       << ", status:" << st.to_string();
            return st;
        }
    }
    if (*deletes == nullptr) {
        return Status::RuntimeError("deletes pointer is null");
    } else {
        (*deletes)->reset_column();
    }
    auto& delidx = indexes[TOpType::DELETE];
    PrimaryKeyEncoder::encode_selective(*_vectorized_schema, *src, delidx.data(), delidx.size(), deletes->get(),
                                        _pk_encoding_type);
    return Status::OK();
}

Status MemTable::_sort_column_inc(bool by_sort_key) {
    Columns columns;
    std::vector<ColumnId> sort_key_idxes;
    if (by_sort_key) {
        sort_key_idxes = _vectorized_schema->sort_key_idxes();
        if (sort_key_idxes.empty()) {
            for (ColumnId i = 0; i < _vectorized_schema->num_key_fields(); ++i) {
                sort_key_idxes.push_back(i);
            }
        }
        if (_keys_type == AGG_KEYS || _keys_type == UNIQUE_KEYS) {
            // check sort_key_idxes is equal to keys
            std::vector<ColumnId> tmp = sort_key_idxes;
            std::sort(tmp.begin(), tmp.end());
            std::vector<ColumnId> key_idxes;
            key_idxes.resize(_vectorized_schema->num_key_fields());
            std::iota(key_idxes.begin(), key_idxes.end(), 0);
            if (!std::equal(tmp.begin(), tmp.end(), key_idxes.begin(), key_idxes.end())) {
                std::string msg = strings::Substitute("tablet type: $0 sort key columns is different with key columns",
                                                      _keys_type);
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            }
        }
    } else {
        for (ColumnId i = 0; i < _vectorized_schema->num_key_fields(); ++i) {
            sort_key_idxes.push_back(i);
        }
    }

    for (auto sort_key_idx : sort_key_idxes) {
        columns.push_back(_chunk->get_column_by_index(sort_key_idx));
    }

    auto sort_descs = SortDescs::asc_null_first(sort_key_idxes.size());
    if (!_merge_condition.empty()) {
        for (int i = 0; i < _vectorized_schema->num_fields(); ++i) {
            if (_vectorized_schema->field(i)->name() == _merge_condition) {
                columns.push_back(_chunk->get_column_by_index(i));
                sort_descs.descs.emplace_back(1, -1);
                break;
            }
        }
    }

    Status st = stable_sort_and_tie_columns(false, columns, sort_descs, &_permutations);
    return st;
}

} // namespace starrocks
