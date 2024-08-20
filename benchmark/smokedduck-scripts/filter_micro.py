from itertools import product
import os
import sys
import duckdb
import pandas as pd
import csv
import argparse
from utils import get_op_timings, Run, DropLineageTables, getStats, MicroDataSelective

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--show_output', action='store_true',  help="Print query output")
parser.add_argument('--show_tables', action='store_true',  help="Print query output")
parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
parser.add_argument('--notes', type=str,  help="exp notes", default="")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")

args = parser.parse_args()
args.profile = True
args.stats = True

pd.set_option('display.max_rows', None)

################### Filter ###########################
# predicate: z=0
# vary: cardinality, selectivity
# operators: Filter (FILTER), and Scan with filter push down (SEQ_SCAN)
########################################################
def FilterMicro(con, args, selectivity, cardinality, results, pushdown):
    print("------------ Test Filter zipfan 1  filter_pushdown: ", pushdown)
    op_name = "FILTER"
    if pushdown:
        op_name = "SEQ_SCAN"
        con.execute("PRAGMA enable_filter_pushdown")
    else:
        con.execute("PRAGMA disable_filter_pushdown")

    for sel, card in product(selectivity, cardinality):
        args.qid=f"Filter_sel{sel}card{card}"
        sel_str = f"{sel}".replace(".", "_")
  
        q = f"SELECT * FROM micro_table_{sel_str}_{card} WHERE z=0"
        avg, df = Run(q, args, con)

        lineage_size, lineage_count, nchunks, postprocess_time  = 0, 0, 0, 0
        if args.lineage:
            lineage_size, lineage_count, nchunks, postprocess_time, plan = getStats(con, q)
            postprocess_time = postprocess_time*1000

        op_timings = get_op_timings(args.qid, op_name)
        results.append([op_name, avg, card, sel, len(df), lineage_size, lineage_count, nchunks, postprocess_time, op_timings])

        if args.lineage:
            DropLineageTables(con)

    con.execute("PRAGMA enable_filter_pushdown")

################### Filter ###############################
# Test filtering on values on z varying cardinality (card)
# and selectivity (sel)
##########################################################
# Creating connection
db_name = 'micro_db.out'
selectivity = [0.02, 0.2, 0.5, 1.0]
cardinality = [10000000] #, 5000000, 10000000]
if not os.path.exists(db_name):
    print("get data")
    con = duckdb.connect(db_name)
    MicroDataSelective(con, selectivity, cardinality)
else:
    con = duckdb.connect(db_name)

results = []
pushdown = False
FilterMicro(con, args, selectivity, cardinality, results, pushdown)
pushdown = True
FilterMicro(con, args, selectivity, cardinality, results, pushdown)
print(results)

if args.save_csv:
    filename="filter_micro_benchmark_{}.csv".format(args.notes)
    print(filename)
    header = ['op_name', 'runtime_ms', 'card', 'sel', 'out_size', 'lineage_size_mb',
    'lineage_count', 'nchunks', 'postprocess_ms', 'filter_ms']
    control = 'w'
    if args.csv_append:
        control = 'a'
    add_header = not os.path.exists(filename)
    if not add_header and control == 'w':
        add_header = True
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        if add_header:
            csvwriter.writerow(header)
        csvwriter.writerows(results)
