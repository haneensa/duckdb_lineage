//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/lineage/lineage_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#ifdef LINEAGE
#pragma once

#include "operator_lineage.hpp"

namespace duckdb {

class ClientContext;
class PhysicalOperator;
class LineageManager;

// Declaration of the global and thread_local variables
extern unique_ptr<LineageManager> lineage_manager;
extern thread_local Log* active_log;
extern thread_local OperatorLineage* pactive_lop;

//! LineageManager
/*!
    LineageManager is xxx
*/
class LineageManager {
public:
	explicit LineageManager() : smoke(false), capture(false), persist(false), enable_filter_pushdown(true), enable_short(true) {}
	void InitOperatorPlan(ClientContext &context, PhysicalOperator *op);
	shared_ptr<OperatorLineage> CreateOperatorLineage(ClientContext &context, PhysicalOperator *op);
	int PlanAnnotator(PhysicalOperator *op, int counter);
	void CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id);
	void StoreQueryLineage(ClientContext &context, PhysicalOperator* op, string query);
	void Reset() {
		active_log = nullptr;
    pactive_lop = nullptr;
	}

  void InitLog(shared_ptr<OperatorLineage> lop, void* thread_id);
  
  void SetP(OperatorLineage* lop, void* thread_id) {
		if (!capture || lop == nullptr) return;
    pactive_lop = lop;
		std::lock_guard<std::mutex> lock(lop->glock);
		active_log = pactive_lop->log[thread_id].get();
    // TODO: access child log to get out_start
    return;

	}

  void PostProcess(shared_ptr<OperatorLineage> lop);

  std::vector<int64_t> GetStats(shared_ptr<OperatorLineage> lop);

	void Clear() {
		Reset();
		queryid_to_plan.clear();
		query_to_id.clear();
		global_logger.clear();
		operators_ids.clear();
    table_lineage_op.clear();
	}

public:
  bool smoke;
  bool capture;
  bool persist;
  std::unordered_map<void*, shared_ptr<OperatorLineage>> global_logger;
  std::unordered_map<void*, int> operators_ids;

  //! map between lineage relational table name and its in-mem lineage
  std::unordered_map<string, shared_ptr<OperatorLineage>> table_lineage_op;
  std::vector<string> query_to_id;
  std::unordered_map<idx_t, shared_ptr<OperatorLineage>> queryid_to_plan;
  std::mutex glock;

  // micro benchmark flags
  bool enable_filter_pushdown;
  bool enable_short;
  unique_ptr<string> explicit_join_type = nullptr;
  unique_ptr<string> explicit_agg_type = nullptr;
};

} // namespace duckdb
#endif
