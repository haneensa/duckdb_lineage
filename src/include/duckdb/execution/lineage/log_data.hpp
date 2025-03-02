//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/log_data.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

enum class PhysicalOperatorType : uint8_t;

//! LogIndex
/*!
    LogIndex is xxx
*/
class LogIndex {
public:
	explicit LogIndex() : offset(0) {}

public:
	vector<idx_t> vals;
	vector<idx_t> vals_2; // for binary relations
  unordered_map<data_ptr_t, idx_t> codes;
  unordered_map<idx_t, idx_t> perfect_codes;
  idx_t offset;
};

struct limit_artifact {
  idx_t start;
  idx_t end;
  idx_t in_start;
};

struct filter_artifact {
	sel_t* sel;
	idx_t count;
	idx_t in_start;
};

struct perfect_join_artifact {
	sel_t* left;
	sel_t* right;
	idx_t count;
	idx_t in_start;
};

struct scan_artifact {
	sel_t* sel;
	idx_t count;
	idx_t start;
	idx_t vector_index;
};

struct address_artifact {
	unique_ptr<data_ptr_t[]> addresses;
	idx_t count;
};

struct no_address_artifact {
	data_ptr_t* addresses;
	idx_t count;
};

struct int_address_artifact {
	int* addresses;
	idx_t count;
};

struct address_sel_artifact {
	data_ptr_t* addresses;
	sel_t* sel;
	idx_t count;
  idx_t in_start;
};

struct combine_artifact {
	unique_ptr<data_ptr_t[]> src;
	unique_ptr<data_ptr_t[]> target;
	idx_t count;
};

struct join_gather_artifact {
	data_ptr_t* rhs;
	sel_t* lhs;
	idx_t count;
  idx_t in_start;
};

struct perfect_full_scan_ht_artifact {
	buffer_ptr<SelectionData> sel_build;
	buffer_ptr<SelectionData> sel_tuples;
  buffer_ptr<VectorBuffer> row_locations;
	idx_t key_count;
	idx_t ht_count;
};

// Cross Product Log
//
struct cross_artifact {
  // returns if the left side is scanned as a constant vector
  uint32_t position_in_chunk;
  uint32_t scan_position;
  uint32_t count;
  uint32_t in_start;
  bool branch_scan_lhs;
};

struct bnlj_artifact {
  // returns if the left side is scanned as a constant vector
  sel_t* sel;
  uint32_t count;
};

// NLJ Log
//
struct nlj_artifact_uniq {
  unsafe_unique_array<sel_t> left;
  unsafe_unique_array<sel_t> right;
  idx_t count;
  idx_t current_row_index;
  idx_t out_start;
};


//! Log
/*!
    Log is xxx
*/
class Log {
public:
	explicit Log() : capture(false) {}

  std::pair<int, int> LatestLSN();
  void SetLatestLSN(std::pair<int, int>);
  ~Log();
  
public:
  bool capture;
	std::vector<filter_artifact> filter_log;
	std::vector<int> all_filter_log;
	std::vector<limit_artifact> limit_offset;
	vector<perfect_full_scan_ht_artifact> perfect_full_scan_ht_log;
  vector<perfect_join_artifact> perfect_probe_ht_log;
	vector<scan_artifact> row_group_log;
	vector<no_address_artifact> scatter_log;
	vector<int_address_artifact> int_scatter_log;
	vector<address_sel_artifact> scatter_sel_log;
	vector<address_artifact> gather_log;
	vector<combine_artifact> combine_log;
	vector<address_artifact> finalize_states_log;
	vector<join_gather_artifact> join_gather_log;
  vector<vector<idx_t>> reorder_log;
  vector<cross_artifact> cross_log;
  //vector<std::array<uint32_t, 5>> no_cross_log;
  vector<nlj_artifact_uniq> nlj_log;
  vector<bnlj_artifact> bnlj_log;

  vector<std::pair<int, int>> execute_internal;
  vector<std::pair<int, int>> cached;

  std::pair<int, int> latest;
  int tuple_size = 0;
  uintptr_t fixed = 0;
private:
};

} // namespace duckdb
#endif
