# python3 tpch_capture.py feb11  --repeat 3  --save_csv  --perm --csv_append
# python3 tpch_capture.py feb11  --repeat 3  --save_csv  --csv_append
import json
import duckdb
import pandas as pd
import argparse
import csv
import os

from utils import parse_plan_timings, Run, DropLineageTables, getStats

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('notes', type=str,  help="run notes")
parser.add_argument('--lineage', action='store_true',  help="Enable lineage")
parser.add_argument('--show_tables', action='store_true',  help="List tables")
parser.add_argument('--show_output', action='store_true',  help="Print query output")
parser.add_argument('--stats', action='store_true',  help="Get lineage size, nchunks and postprocess time")
parser.add_argument('--query_lineage', action='store_true',  help="query lineage")
parser.add_argument('--perm', action='store_true',  help="use perm queries")
parser.add_argument('--gprom', action='store_true',  help="use perm queries")
parser.add_argument('--opt', action='store_true',  help="use optimized")
parser.add_argument('--save_csv', action='store_true',  help="save result in csv")
parser.add_argument('--csv_append', action='store_true',  help="Append results to old csv")
parser.add_argument('--sf', type=float, help="sf scale", default=1)
parser.add_argument('--repeat', type=int, help="Repeat time for each query", default=1)
parser.add_argument('--folder', type=str, help='queries folder', default='benchmark/smokedduck//')
args = parser.parse_args()
args.profile = True
print(args.profile)

con = duckdb.connect(database=':memory:', read_only=False)
prefix = args.folder + "queries/q"
table_name=None
size_avg = 0.0
if args.perm:
    prefix = args.folder + "queries/perm/q"
    args.lineage_query = False
    lineage_type = "Logical-RID"
    table_name='lineage'
    if args.opt:
        prefix = args.folder + "queries/optimized_perm/q"
        lineage_type = "Logical-OPT"
elif args.gprom:
    prefix = args.folder + "queries/gprom/q"
    args.lineage_query = False
    lineage_type = "Logical-window"
    table_name='lineage'
elif not args.lineage:
    lineage_type = "Baseline"
else:
    lineage_type = "SD_Capture"
# sf: 1, 5, 10, 20
# threads: 1, 4, 8, 12, 16
threads_list = [1, 4, 8, 16]
opt_queries = [2, 4, 15, 16, 17, 20, 21]
dont_scale = [2, 4, 17, 20, 21] #, 4, 16, 17, 20, 21, 22]
dont_scale_10 = [11, 22]
dont_scale_20 = [11, 16, 22]
gprom_list = [1, 2, 4, 5, 7, 9, 11, 12, 13, 15, 22]

# 4, 16, 18, 20, 21, 22 join is empty. check why.
# 11 nested loop join

# 3, 7, 4
results = []
sf = args.sf
con.execute("CALL dbgen(sf="+str(sf)+");")
for th_id in threads_list:
    con.execute("PRAGMA threads="+str(th_id))
    for i in range(1, 23):
        if args.gprom and i not in gprom_list: continue
        if (args.perm and args.opt == False) and ((i in dont_scale) or (sf>10 and i in dont_scale_20) or (sf==10 and i in dont_scale_10)): continue
        if (args.gprom) and   (sf>10 and i in dont_scale_20 or i==1) or (sf==10 and i in dont_scale_10): continue
        if args.perm and args.opt and i not in opt_queries: continue
        args.qid = i
        qfile = prefix+str(i).zfill(2)+".sql"
        text_file = open(qfile, "r")
        query = text_file.read().strip()
        query = ' '.join(query.split())
        text_file.close()
        print("%%%%%%%%%%%%%%%% Running Query # ", i, " threads: ", th_id)
        print(query)
        avg, df = Run(query, args, con, table_name)
        plan_timings = None
        if args.profile:
            plan_timings = parse_plan_timings(args.qid)
        output_size = len(df)
        if table_name:
            df = con.execute("select count(*) as c from {}".format(table_name)).fetchdf()
            output_size = df.loc[0,'c']
            con.execute("DROP TABLE "+table_name)
        print("**** output size: ", output_size)
        if args.show_tables:
            tables = con.execute("PRAGMA show_tables").fetchdf()
            print(tables)
            if args.lineage:
                for t in tables['name']:
                    if "LINEAGE" in t:
                        print(t)
                        print(con.execute(f"select * from {t}").df())
        stats = ""

        if args.lineage and args.stats:
            lineage_size, lineage_count, nchunks, postprocess_time, plan = getStats(con, query)
            print(plan)
            size_avg += lineage_size
            stats = "{},{},{},{}".format(lineage_size, lineage_count, nchunks, postprocess_time*1000)
        if args.lineage:
            DropLineageTables(con)
        results.append([i, avg, sf, args.repeat, lineage_type, th_id, output_size, stats, args.notes,plan_timings])
print("average", size_avg/22.0)
if args.save_csv:
    filename="tpch_benchmark_capture_{}.csv".format(args.notes)
    print(filename)
    header = ["query", "runtime", "sf", "repeat", "lineage_type", "n_threads", "output", "stats", "notes", "plan_timings"]
    control = 'w'
    if args.csv_append:
        control = 'a'
    with open(filename, control) as csvfile:
        csvwriter = csv.writer(csvfile)
        if not args.csv_append:
            csvwriter.writerow(header)
        csvwriter.writerows(results)
