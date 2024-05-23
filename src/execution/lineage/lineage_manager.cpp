#ifdef LINEAGE

#include "duckdb/execution/lineage/lineage_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"

namespace duckdb {

unique_ptr<LineageManager> lineage_manager;
thread_local Log* active_log;
thread_local shared_ptr<OperatorLineage> active_lop;

shared_ptr<OperatorLineage> LineageManager::CreateOperatorLineage(ClientContext &context, PhysicalOperator *op) {
	global_logger[(void*)op] = make_shared_ptr<OperatorLineage>(operators_ids[(void*)op], op->type, op->GetName());

	if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, plan);
		global_logger[(void*)op]->children.push_back(lop);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		auto distinct = (PhysicalOperator*)dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get();
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, distinct);
		global_logger[(void*)op]->children.push_back(lop);
		for (idx_t i = 0; i < dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans.size(); ++i) {
			// dynamic_cast<PhysicalDelimJoin *>(op)->delim_scans[i]->lineage_op = distinct->lineage_op;
		}
		lop = CreateOperatorLineage(context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get());
		global_logger[(void*)op]->children.push_back(lop);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		shared_ptr<OperatorLineage> lop = CreateOperatorLineage(context, op->children[i].get());
		global_logger[(void*)op]->children.push_back(lop);
	}

	return global_logger[(void*)op];
}

// Iterate through in Postorder to ensure that children have PipelineLineageNodes set before parents
int LineageManager::PlanAnnotator(PhysicalOperator *op, int counter) {
	
	operators_ids[(void*)op] = counter;

  if (op->type == PhysicalOperatorType::RESULT_COLLECTOR) {
		PhysicalOperator* plan = &dynamic_cast<PhysicalResultCollector*>(op)->plan;
		counter = PlanAnnotator(plan, counter);
	}

  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		counter = PlanAnnotator( dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), counter);
		counter = PlanAnnotator((PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), counter);
	}

	for (idx_t i = 0; i < op->children.size(); i++) {
		counter = PlanAnnotator(op->children[i].get(), counter);
	}

	return counter + 1;
}

void LineageManager::InitOperatorPlan(ClientContext &context, PhysicalOperator *op) {
	if (!capture) return;
	PlanAnnotator(op, 0);
	CreateOperatorLineage(context, op);
}

void LineageManager::CreateLineageTables(ClientContext &context, PhysicalOperator *op, idx_t query_id) {
  if (op->type == PhysicalOperatorType::RIGHT_DELIM_JOIN || op->type == PhysicalOperatorType::LEFT_DELIM_JOIN) {
		CreateLineageTables( context, dynamic_cast<PhysicalDelimJoin *>(op)->join.get(), query_id);
		CreateLineageTables(context, (PhysicalOperator*) dynamic_cast<PhysicalDelimJoin *>(op)->distinct.get(), query_id);
	}
	for (idx_t i = 0; i < op->children.size(); i++) {
		CreateLineageTables(context, op->children[i].get(), query_id);
	}

	OperatorLineage *lop = lineage_manager->global_logger[(void *)op].get();
	if (lop == nullptr) return;
  lop->extra = op->ParamsToString();

  if (op->type == PhysicalOperatorType::TABLE_SCAN) {
		string table_str = dynamic_cast<PhysicalTableScan *>(op)->ParamsToString();
		lop->table_name = table_str.substr(0, table_str.find('\n'));
	}

	auto table_column_types = lop->GetTableColumnTypes();
	if (table_column_types.empty()) return;

	// Example: LINEAGE_1_HASH_JOIN_3
	string prefix = "LINEAGE_" + to_string(query_id) + "_" + op->GetName() + "_" + to_string(lop->operator_id);
	prefix.erase( remove( prefix.begin(), prefix.end(), ' ' ), prefix.end() );
	// add column_stats, cardinality
	string catalog_name = TEMP_CATALOG;
	auto binder = Binder::CreateBinder(context);
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	for (idx_t i = 0; i < table_column_types.size(); i++) {
		if (table_column_types[i].size() == 0) continue;
		// Example: LINEAGE_1_HASH_JOIN_3
		string table_name = prefix;
		// Create Table
		auto create_info = make_uniq<CreateTableInfo>(catalog_name, DEFAULT_SCHEMA, table_name);
    create_info->temporary = true;
	  create_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		for (idx_t col_i = 0; col_i < table_column_types[i].size(); col_i++) {
			create_info->columns.AddColumn(move(table_column_types[i][col_i]));
		}
		table_lineage_op[table_name] = lineage_manager->global_logger[(void *)op];
		catalog.CreateTable(context, move(create_info));
	}
}

void LineageManager::StoreQueryLineage(ClientContext &context, PhysicalOperator *op, string query) {
	if (!capture)
		return;

	idx_t query_id = query_to_id.size();
	query_to_id.push_back(query);
	queryid_to_plan[query_id] = lineage_manager->global_logger[(void *)op];
  if (persist) CreateLineageTables(context, op, query_id);
}

} // namespace duckdb
#endif
