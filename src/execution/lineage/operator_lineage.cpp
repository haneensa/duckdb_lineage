#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

void OperatorLineage::PostProcess() {
  if (processed) return;
	thread_vec.reserve(log.size());
	for (const auto& pair : log) {
		thread_vec.push_back(pair.first);
	}
	switch (type) {
	case PhysicalOperatorType::FILTER: {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->filter_log.empty()) continue;
      for (int k=0; k < log[tkey]->filter_log.size(); ++k) {
        idx_t res_count = log[tkey]->filter_log[k].count;
        idx_t offset = log[tkey]->filter_log[k].in_start;
        if (log[tkey]->filter_log[k].sel) {
          auto payload = log[tkey]->filter_log[k].sel.get();
          for (idx_t j=0; j < res_count; ++j) {
              payload[j] += offset;
          }
        }
      }
    }
    break;
                                     }
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT: {
    break;
                                    }
	case PhysicalOperatorType::TABLE_SCAN: {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->row_group_log.empty()) continue;
      for (int k=0; k < log[tkey]->row_group_log.size(); ++k) {
        idx_t res_count = log[tkey]->row_group_log[k].count;
        idx_t offset = log[tkey]->row_group_log[k].start + log[tkey]->row_group_log[k].vector_index;
        if (log[tkey]->row_group_log[k].sel) {
          auto payload = log[tkey]->row_group_log[k].sel->owned_data.get();
          for (idx_t j=0; j < res_count; ++j) {
            payload[j] += offset;
          }
        }
      }
    }
    break;
                                         }
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    // gather
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->finalize_states_log.empty()) continue;
      // std::cout << "finalize states: " << tkey << " " << log[tkey]->finalize_states_log.size() << std::endl;
      idx_t count_so_far = 0;
      for (int k=0; k < log[tkey]->finalize_states_log.size(); ++k) {
        idx_t res_count = log[tkey]->finalize_states_log[k].count;
        auto payload = log[tkey]->finalize_states_log[k].addresses.get();
        for (idx_t j=0; j < res_count; ++j) {
          if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
            log_index->codes[payload[j]] = j + count_so_far;
            // TODO: add tkey associasted with this code
            //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
          }
        }
        count_so_far += res_count;
      }
      // free gather
      log[tkey]->finalize_states_log.clear();
    }      
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->combine_log.empty()) continue;
      //std::cout << "combine states: " << log[tkey]->combine_log.size() << std::endl;
      for (int k=0; k < log[tkey]->combine_log.size(); ++k) {
        idx_t res_count = log[tkey]->combine_log[k].count;
        auto src = log[tkey]->combine_log[k].src.get();
        auto target = log[tkey]->combine_log[k].target.get();
        for (idx_t j=0; j < res_count; ++j) {
          log_index->codes[src[j]] = log_index->codes[target[j]];
        }
      }
      // free combine
      log[tkey]->combine_log.clear();
    }      

    std::cout << " done " << std::endl;
    break;
  }
	case PhysicalOperatorType::ORDER_BY: {
    idx_t count_so_far = 0;
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->reorder_log.empty()) continue;
      for (int k = 0; k < log[tkey]->reorder_log.size(); k++) {
        // std::cout << k << "Order : " << log[tkey]->reorder_log.size() << " " << count_so_far << " " << log[tkey]->reorder_log[k].size() << std::endl;
        idx_t res_count = log[tkey]->reorder_log[k].size();
        for (idx_t j=0; j < res_count; ++j) {
            log_index->vals.push_back( log[tkey]->reorder_log[k][j] + count_so_far );
        }
      }
      log[tkey]->reorder_log.clear();
    }
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      idx_t count_so_far = 0;
      if (log.count(tkey) == 0 || log[tkey]->scatter_sel_log.empty()) continue;
      // std::cout << "Scatter Join: " << log[tkey]->scatter_sel_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->scatter_sel_log.size(); k++) {
        idx_t res_count = log[tkey]->scatter_sel_log[k].count;
        auto payload = log[tkey]->scatter_sel_log[k].addresses.get();
        //std::cout << k << " " << res_count << std::endl;
        if (log[tkey]->scatter_sel_log[k].sel) {
          auto sel = log[tkey]->scatter_sel_log[k].sel.get();
          for (idx_t j=0; j < res_count; ++j) {
            if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
              log_index->codes[payload[j]] = sel[j] + count_so_far;
              //st9::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
            }
          }
        } else {
          for (idx_t j=0; j < res_count; ++j) {
            if (log_index->codes.find(payload[j]) == log_index->codes.end()) {
              log_index->codes[payload[j]] = j + count_so_far;
              //std::cout << "gather: " << k << " " << log_index->codes[payload[j]] << " " << j << " " << (void*)payload[j] << std::endl;
            }
          }
        }
      }
    }
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->perfect_full_scan_ht_log.empty()) continue;
     // std::cout << "Perfect Join: " << log[tkey]->perfect_full_scan_ht_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->perfect_full_scan_ht_log.size(); k++) {
        idx_t key_count = log[tkey]->perfect_full_scan_ht_log[k].key_count;
        idx_t ht_count = log[tkey]->perfect_full_scan_ht_log[k].ht_count;
       //  std::cout << k << " " << key_count << " " << ht_count << std::endl;
        for (int e=0; e < key_count; e++) {
          idx_t build_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_build->owned_data.get()[e];
          idx_t tuples_idx = log[tkey]->perfect_full_scan_ht_log[k].sel_tuples->owned_data.get()[e];
          data_ptr_t* ptr = (data_ptr_t*)log[tkey]->perfect_full_scan_ht_log[k].row_locations->GetData();
         // std::cout << "-> " << build_idx << " " << tuples_idx << " " << key_count << " " << ht_count  << std::endl;
         // std::cout << "-> " << (void*)ptr[ tuples_idx ]  << std::endl;
        }
      }
    }
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      idx_t input_offset = 0;
      if (log.count(tkey) == 0 || log[tkey]->perfect_probe_ht_log.empty()) continue;
      // std::cout << "Perfect Probe Join: " << log[tkey]->perfect_probe_ht_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->perfect_probe_ht_log.size(); k++) {
        idx_t count = log[tkey]->perfect_probe_ht_log[k].count;
       // std::cout << k << " " << count << std::endl;
        auto left = log[tkey]->perfect_probe_ht_log[k].left.get();
        auto right = log[tkey]->perfect_probe_ht_log[k].right.get();
        if (left == nullptr) {
          for (idx_t j=0; j < count; ++j) {
            log_index->vals.push_back( log_index->codes[ log_index->perfect_codes[ right[j] ] ] );
            log_index->vals_2.push_back( j + input_offset );
          }
        } else {
          for (idx_t j=0; j < count; ++j) {
            log_index->vals.push_back( log_index->codes[ log_index->perfect_codes[ right[j] ] ] );
            log_index->vals_2.push_back( left[j] + input_offset );
          }
        }
      }
    }
    for (int i=0; i < thread_vec.size(); i++) {
      void* tkey = thread_vec[i];
      if (log.count(tkey) == 0 || log[tkey]->join_gather_log.empty()) continue;
      // std::cout << "Join: " << log[tkey]->join_gather_log.size() << std::endl;
      for (int k = 0; k < log[tkey]->join_gather_log.size(); k++) {
        idx_t res_count = log[tkey]->join_gather_log[k].count;
        idx_t in_start = log[tkey]->join_gather_log[k].in_start;
        auto payload = log[tkey]->join_gather_log[k].rhs.get();
        //std::cout << k << " " << res_count << std::endl;
        for (idx_t j=0; j < res_count; ++j) {
          log_index->vals.push_back( log_index->codes[ payload[j] ] );
          log_index->vals_2.push_back( j + in_start );
        }
      }
    }
    break;
  }
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	processed = true;
}

//! Get the column types for this operator
//! Returns 1 vector of ColumnDefinitions for each table that must be created
vector<ColumnDefinition> OperatorLineage::GetTableColumnTypes() {
	vector<ColumnDefinition> source;
	switch (type) {
	case PhysicalOperatorType::COLUMN_DATA_SCAN:
	case PhysicalOperatorType::FILTER:
	case PhysicalOperatorType::TABLE_SCAN: {
    source.emplace_back("in_index", LogicalType::INTEGER);
    source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("partition_index", LogicalType::INTEGER);
    break;
  }
	case PhysicalOperatorType::STREAMING_LIMIT:
	case PhysicalOperatorType::LIMIT:
	case PhysicalOperatorType::ORDER_BY: {
    source.emplace_back("in_index", LogicalType::BIGINT);
    source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("partition_index", LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    source.emplace_back("in_index", LogicalType::BIGINT);
    source.emplace_back("out_index", LogicalType::INTEGER);
    source.emplace_back("sink_index", LogicalType::INTEGER);
    source.emplace_back("getdata_index", LogicalType::INTEGER);
		break;
	}
	case PhysicalOperatorType::HASH_JOIN:
	case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
	case PhysicalOperatorType::CROSS_PRODUCT:
	case PhysicalOperatorType::NESTED_LOOP_JOIN:
	case PhysicalOperatorType::PIECEWISE_MERGE_JOIN: {
		source.emplace_back("lhs_index", LogicalType::BIGINT);
		source.emplace_back("rhs_index", LogicalType::BIGINT);
		source.emplace_back("out_index", LogicalType::BIGINT);
    source.emplace_back("sink_index", LogicalType::INTEGER);
    source.emplace_back("getdata_index", LogicalType::INTEGER);
		break;
	}
	default: {
		// Lineage unimplemented! TODO all of these :)
	}
	}
	return source;
}

idx_t OperatorLineage::GetLineageAsChunk(DataChunk &insert_chunk,
                        idx_t& global_count, idx_t& local_count,
                        idx_t &thread_id, idx_t &data_idx,  bool &cache) {
	auto table_types = GetTableColumnTypes();
	vector<LogicalType> types;

	for (const auto& col_def : table_types) {
		types.push_back(col_def.GetType());
	}

	insert_chunk.InitializeEmpty(types);
	if (thread_vec.size() <= thread_id) {
		return 0;
	}

	void* thread_val  = thread_vec[thread_id];
	GetLineageAsChunkLocal(data_idx, local_count, insert_chunk, thread_id, log[thread_val]);

	global_count += insert_chunk.size();
	local_count += insert_chunk.size();
	data_idx++;

	if (insert_chunk.size() == 0) {
		thread_id++;
		cache = true;
    local_count = 0;
		data_idx = 0;
	}

	return insert_chunk.size();
}

idx_t OperatorLineage::GetLineageAsChunkLocal(idx_t data_idx, idx_t global_count,
    DataChunk& chunk, int thread_id, shared_ptr<Log> log) {
	if (log == nullptr) return 0;
  // std::cout << "get lineage as chunk : " << data_idx << " " << global_count << std::endl;

  Vector thread_id_vec(Value::INTEGER(thread_id));
	switch (type) {
	// schema: [INTEGER in_index, INTEGER out_index, INTEGER partition_index]
	case PhysicalOperatorType::FILTER: {
    if (data_idx >= log->filter_log.size()) return 0;
    idx_t count = log->filter_log[data_idx].count;
    idx_t offset = log->filter_log[data_idx].in_start;
    data_ptr_t ptr = nullptr;
    if (log->filter_log[data_idx].sel) {
      ptr = (data_ptr_t)log->filter_log[data_idx].sel.get();
    }

    chunk.SetCardinality(count);
    if (ptr != nullptr) {
      Vector in_index(LogicalType::INTEGER, ptr);
      chunk.data[0].Reference(in_index);
    } else {
      chunk.data[0].Sequence(offset, 1, count); // in_index
    }
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
	case PhysicalOperatorType::TABLE_SCAN: {
    if (data_idx >= log->row_group_log.size()) return 0;
    idx_t count = log->row_group_log[data_idx].count;
    idx_t offset = log->row_group_log[data_idx].start + log->row_group_log[data_idx].vector_index;
    data_ptr_t ptr = nullptr;
    if (log->row_group_log[data_idx].sel) {
      ptr = (data_ptr_t)log->row_group_log[data_idx].sel->owned_data.get();
    }
    chunk.SetCardinality(count);
    if (ptr != nullptr) {
      Vector in_index(LogicalType::INTEGER, ptr);
      chunk.data[0].Reference(in_index);
    } else {
      chunk.data[0].Sequence(offset, 1, count); // in_index
    }
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
  case PhysicalOperatorType::STREAMING_LIMIT:
  case PhysicalOperatorType::LIMIT: {
    if (data_idx >= log->limit_offset.size()) return 0;

    idx_t start = log->limit_offset[data_idx].start;
    idx_t count = log->limit_offset[data_idx].end;
    idx_t offset = log->limit_offset[data_idx].in_start;
    chunk.SetCardinality(count);
    chunk.data[0].Sequence(start+offset, 1, count); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    break;
  }
	case PhysicalOperatorType::HASH_GROUP_BY:
	case PhysicalOperatorType::PERFECT_HASH_GROUP_BY: {
    if (data_idx >= log->scatter_log.size()) return 0;
    idx_t count = log->scatter_log[data_idx].count;
    chunk.SetCardinality(count);

    auto payload = log->scatter_log[data_idx].addresses.get();
    chunk.data[1].Initialize(false, count);
    int* out_index_ptr = (int*)chunk.data[1].GetData();
    for (idx_t j=0; j < count; ++j) {
        out_index_ptr[j] = (int)log_index->codes[ payload[j] ];
    }
    chunk.data[0].Sequence(global_count, 1, count); // in_index
    chunk.data[2].Reference(thread_id_vec);
    chunk.data[3].Reference(thread_id_vec);
		break;
	}
	case PhysicalOperatorType::ORDER_BY: {
    if (log_index->offset >= log_index->vals.size()) return 0;
    idx_t count = log_index->vals.size() - log_index->offset;
    if (count > STANDARD_VECTOR_SIZE) {
      count = STANDARD_VECTOR_SIZE;
    }
    data_ptr_t ptr = (data_ptr_t)(log_index->vals.data() + log_index->offset);
    chunk.SetCardinality(count);
    Vector in_index(LogicalType::BIGINT, ptr);
    chunk.data[0].Reference(in_index); // in_index
    chunk.data[1].Sequence(global_count, 1, count); // out_index
    chunk.data[2].Reference(thread_id_vec);
    log_index->offset += count;
		break;
	}
	case PhysicalOperatorType::HASH_JOIN: {
    if (log_index->offset >= log_index->vals.size()) return 0;
    idx_t count = log_index->vals.size() - log_index->offset;
    if (count > STANDARD_VECTOR_SIZE) {
      count = STANDARD_VECTOR_SIZE;
    }
    chunk.SetCardinality(count);
    data_ptr_t lhs_ptr = (data_ptr_t)(log_index->vals.data() + log_index->offset);
    Vector lhs_index(LogicalType::BIGINT, lhs_ptr);

    data_ptr_t rhs_ptr = (data_ptr_t)(log_index->vals_2.data() + log_index->offset);
    Vector rhs_index(LogicalType::BIGINT, rhs_ptr);
    chunk.data[0].Reference(lhs_index); // lhs_index
    chunk.data[1].Reference(rhs_index); // rhs_index
    chunk.data[2].Sequence(global_count, 1, count); // out_index
    chunk.data[3].Reference(thread_id_vec);
    chunk.data[4].Reference(thread_id_vec);
    log_index->offset += count;
		break;
	}
	default: {
		// Not Implemented
	}
	}

	return chunk.size();
}

} // namespace duckdb
#endif
