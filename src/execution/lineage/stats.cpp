#ifdef LINEAGE

#include "duckdb/execution/lineage/operator_lineage.hpp"

namespace duckdb {

std::vector<int64_t> OperatorLineage::GatherStats() {
  // stats
  int64_t lineage_size_mb = 0;
  int64_t tuples_count = 0;
  int64_t chunk_count = 0;

  if (!thread_vec.empty()) return {lineage_size_mb, tuples_count, chunk_count};

	thread_vec.reserve(log.size());
	for (const auto& pair : log) {
		thread_vec.push_back(pair.first);
	}
  
  for (int i=0; i < thread_vec.size(); i++) {

    int64_t local_lineage_size_mb = 0;
    int64_t local_tuples_count = 0;
    void* tkey = thread_vec[i];
    if ( log.count(tkey)  == 0) continue;

    // filter log
    // src/execution/operator/filter/physical_filter.cpp
    chunk_count += log[tkey]->filter_log.size();
    local_lineage_size_mb += chunk_count * sizeof(filter_artifact);
    for (int k=0; k < log[tkey]->filter_log.size(); ++k) {
      idx_t res_count = log[tkey]->filter_log[k].count;
      if (log[tkey]->filter_log[k].sel) // only add if mem is alloc
        local_lineage_size_mb +=  res_count * sizeof(sel_t);
      local_tuples_count += res_count;
    }

    // filter scan
    // src/storage/table/row_group.cpp
    chunk_count += log[tkey]->row_group_log.size();
    local_lineage_size_mb += chunk_count * sizeof(scan_artifact);
    for (int k=0; k < log[tkey]->row_group_log.size(); ++k) {
      idx_t res_count = log[tkey]->row_group_log[k].count;
      if (log[tkey]->row_group_log[k].sel) // only add if mem is alloc
        local_lineage_size_mb += res_count * sizeof(sel_t);
      local_tuples_count += res_count;
    }
    
    // finalize states
    for (int k=0; k < log[tkey]->finalize_states_log.size(); ++k) {
      idx_t res_count = log[tkey]->finalize_states_log[k].count;
      local_lineage_size_mb += sizeof(address_artifact) + res_count * sizeof(data_ptr_t);
    }
      
    // combine_log
    for (int k=0; k < log[tkey]->combine_log.size(); ++k) {
      idx_t res_count = log[tkey]->combine_log[k].count;
      local_lineage_size_mb += sizeof(combine_artifact) + 2 * (res_count * sizeof(data_ptr_t));
    }
      
    // scatter_log
    chunk_count += log[tkey]->scatter_log.size();
    for (int k=0; k < log[tkey]->scatter_log.size(); ++k) {
      idx_t res_count = log[tkey]->scatter_log[k].count;
      local_lineage_size_mb += sizeof(address_artifact) +  (res_count * sizeof(data_ptr_t));
      local_tuples_count += res_count;
    }

    // reorder_log
    chunk_count += log[tkey]->reorder_log.size();
    for (int k = 0; k < log[tkey]->reorder_log.size(); k++) {
      idx_t res_count = log[tkey]->reorder_log[k].size();
      local_lineage_size_mb += res_count * sizeof(idx_t);
      local_tuples_count += res_count;
    }

    // scatter_sel_log
    chunk_count += log[tkey]->scatter_sel_log.size();
    for (int k = 0; k < log[tkey]->scatter_sel_log.size(); k++) {
      idx_t res_count = log[tkey]->scatter_sel_log[k].count;
      local_lineage_size_mb += sizeof(address_sel_artifact) 
                   +  (res_count * (sizeof(data_ptr_t) + sizeof(sel_t)));
      local_tuples_count += res_count;
    }
    
    // perfect full scan ht
    chunk_count += log[tkey]->perfect_full_scan_ht_log.size();
    for (int k = 0; k < log[tkey]->perfect_full_scan_ht_log.size(); k++) {
      idx_t key_count = log[tkey]->perfect_full_scan_ht_log[k].key_count;
      idx_t ht_count = log[tkey]->perfect_full_scan_ht_log[k].ht_count;
      local_lineage_size_mb += sizeof(perfect_full_scan_ht_artifact) 
                   + 2 * key_count * sizeof(data_ptr_t)
                   + ht_count  * sizeof(sel_t);
    }
    
    // perfect probe ht
    chunk_count += log[tkey]->perfect_probe_ht_log.size();
    for (int k = 0; k < log[tkey]->perfect_probe_ht_log.size(); k++) {
      idx_t count = log[tkey]->perfect_probe_ht_log[k].count;
      auto left = log[tkey]->perfect_probe_ht_log[k].left.get();
      local_lineage_size_mb += count * sizeof(sel_t);
      if (left != nullptr) {
        local_lineage_size_mb += sizeof(perfect_join_artifact) + count * sizeof(sel_t);
      }
    }

    // join gather log
    chunk_count += log[tkey]->join_gather_log.size();
    for (int k = 0; k < log[tkey]->join_gather_log.size(); k++) {
      idx_t res_count = log[tkey]->join_gather_log[k].count;
      local_lineage_size_mb += res_count * sizeof(sel_t);
    }
    
    // nlj log
    chunk_count += log[tkey]->nlj_log.size();
    for (int k=0; k < log[tkey]->nlj_log.size(); ++k) {
      idx_t count = log[tkey]->nlj_log[k].count;
      local_lineage_size_mb += sizeof(nlj_artifact_uniq) 
                   +  (count * (sizeof(sel_t) + sizeof(sel_t)));
      local_tuples_count += count;
      if (log[tkey]->nlj_log[k].left != nullptr) {
        local_lineage_size_mb += count * sizeof(sel_t);
      }
      
      if (log[tkey]->nlj_log[k].right != nullptr) {
        local_lineage_size_mb += count * sizeof(sel_t);
      }
    }
    
    lineage_size_mb += local_lineage_size_mb / (1024 * 1024);
    tuples_count += local_tuples_count;
  }
  
  return {lineage_size_mb, tuples_count, chunk_count};
}

} // namespace duckdb
#endif
