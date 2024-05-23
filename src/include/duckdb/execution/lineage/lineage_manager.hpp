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
#include <mutex>

namespace duckdb {

class ClientContext;
class PhysicalOperator;
class LineageManager;

// Declaration of the global and thread_local variables
extern unique_ptr<LineageManager> lineage_manager;
extern thread_local Log* active_log;
extern thread_local shared_ptr<OperatorLineage> active_lop;

//! LineageManager
/*!
    LineageManager is xxx
*/
class LineageManager {
public:
	explicit LineageManager() : capture(false), persist(false) {}
	void InitOperatorPlan(ClientContext &context, PhysicalOperator *op);
	shared_ptr<OperatorLineage> CreateOperatorLineage(ClientContext &context, PhysicalOperator *op);
	int PlanAnnotator(PhysicalOperator *op, int counter);
	void CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id);
	void StoreQueryLineage(ClientContext &context, PhysicalOperator* op, string query);
	void Reset() {
		active_log = nullptr;
	}

	void Set(void* opid, void* thread_id) {
		std::lock_guard<std::mutex> lock(glock);
		if (capture && global_logger[opid]) {
      active_lop = global_logger[opid];
			if ( active_lop->log.count(thread_id) == 0) {
				active_lop->log[thread_id] = make_uniq<Log>();
			}
			active_log = active_lop->log[thread_id].get();
		}
	}

	void Clear() {
		Reset();
		queryid_to_plan.clear();
		query_to_id.clear();
		global_logger.clear();
		operators_ids.clear();
	}

public:
  bool capture;
  bool persist;
  std::unordered_map<void*, shared_ptr<OperatorLineage>> global_logger;
  std::unordered_map<void*, int> operators_ids;

  //! map between lineage relational table name and its in-mem lineage
  std::unordered_map<string, shared_ptr<OperatorLineage>> table_lineage_op;
  std::vector<string> query_to_id;
  std::unordered_map<idx_t, shared_ptr<OperatorLineage>> queryid_to_plan;
  std::mutex glock;
};

} // namespace duckdb
#endif
