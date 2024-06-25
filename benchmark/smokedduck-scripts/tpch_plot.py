import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from utils import legend_bottom, legend_side, relative_overhead, overhead, getAllExec, getstats, getMat

type1 = ['1', '3', '5', '6', '7', '8', '9', '10', '12', '13', '14', '19']
type2 = ['11', '15', '16', '18']
type3 = ['2', '4', '17', '20', '21', '22']

parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--file', type=str, help='queries folder', default='tpch_benchmark_capture_may27_e.csv')
args = parser.parse_args()

con = duckdb.connect()
df = pd.read_csv(args.file)
header_unique = ["query","sf", "qtype", "lineage_type", "n_threads"]
g = ','.join(header_unique)
    
df.loc[:, "lineage_size"] = df.apply(lambda x: getstats(x, 0), axis=1)
df.loc[:, "lineage_count"] = df.apply(lambda x: getstats(x, 1), axis=1)
df.loc[:, "nchunks"] = df.apply(lambda x: getstats(x, 2), axis=1)
df.loc[:, "postprocess"] = df.apply(lambda x: getstats(x, 3), axis=1)

df.loc[:, "mat_time"] = df.apply(lambda x: getMat(x['plan_timings'], x['query']), axis=1)
df.loc[:, "plan_runtime"] = df.apply(lambda x: getAllExec(x['plan_timings'], x['query']), axis=1)

def cat(qid):
    if qid in type1:
        return "1. Joins-Aggregations"
    elif qid in type2:
        return "2. Uncorrelated subQs"
    else:
        return "3. Correlated subQs"

df["qtype"] = df.apply(lambda x: cat(str(x['query'])), axis=1)
tpch_df = df

print(con.execute("select count(*) from tpch_df").fetchdf())
aug_tpch = con.execute("""select * from tpch_df UNION ALL
                    select query, runtime, sf, repeat, 'Logical-OPT' as lineage_type,
                    n_threads, output, stats, notes, plan_timings,lineage_size, lineage_count,
                    nchunks, postprocess, mat_time, plan_runtime, qtype from tpch_df
                    where lineage_type='Logical-RID'
                          and query not in (select query from tpch_df where lineage_type='Logical-OPT')
                   """).fetchdf()
print(aug_tpch)
avg_tpch = con.execute("""select {},
                            max(nchunks) as nchunks,
                            max(lineage_size) as lineage_size, max(lineage_count) as lineage_count,
                            max(postprocess) as postprocess,
                            avg(plan_runtime) as plan_runtime, avg(runtime) as runtime,
                            avg(output) as output,  avg(mat_time) as mat_time from aug_tpch
                            group by {}""".format(g, g)).fetchdf()
print(avg_tpch)
header_unique.remove("lineage_type")
metrics = ["runtime", "output", "mat_time", "plan_runtime", "lineage_size", "lineage_count", "nchunks", "postprocess"]
g = ','.join(header_unique)
m = ','.join(metrics)
tpch_withbaseline = con.execute(f"""select
                  t1.plan_runtime as base_plan_runtime, t1.runtime as base_runtime,
                  t1.output as base_output, t1.mat_time as base_mat_time,
                  (t1.plan_runtime-t1.mat_time) as base_plan_no_create,
                  (t2.plan_runtime-t2.mat_time) as plan_no_create,
                  t2.* from (select {g}, {m} from avg_tpch where lineage_type='Baseline') as t1 join avg_tpch  as t2 using ({g})
                  """).fetchdf()
print(tpch_withbaseline)

tpch_metrics = con.execute("""
select {},  lineage_type, n_threads, output / base_output as fanout, output, nchunks, lineage_size, lineage_count, postprocess,
(plan_no_create-base_plan_no_create)*1000 as exec_overhead,
((plan_no_create-base_plan_no_create)/base_plan_no_create)*100 as exec_roverhead,
(mat_time - base_mat_time)*1000 as mat_overhead,
((mat_time - base_mat_time) / base_plan_no_create) *100 as mat_roverhead,
(plan_runtime-base_plan_runtime)*1000 as overhead,
((plan_runtime-base_plan_runtime)/base_plan_no_create)*100 as roverhead,
from tpch_withbaseline order by qtype, query, n_threads, lineage_type
                  """.format(g, g, g)).fetchdf()
pd.set_option("display.max_rows", None)

print(tpch_metrics)

def mktemplate(overheadType, prefix, table):
    return f"""
    SELECT '{overheadType}' as overheadType, qtype,
            query as qid, sf, n_threads, output,
            nchunks, lineage_size, lineage_count, postprocess,
           lineage_type as system,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH temp as (
    {mktemplate('Total', '', 'tpch_metrics')}
    UNION ALL
    {mktemplate('Materialize', 'mat_', 'tpch_metrics')}
    UNION ALL
    {mktemplate('Execute', 'exec_', 'tpch_metrics')}
  ) SELECT * FROM temp {"{}"} ORDER BY overheadType desc """

where = f"where overheadtype<>'Materialize' and n_threads=1"
q = template.format(where)
print(q)
data = con.execute(q).fetchdf()

class_list = type1
class_list.extend(type2)
class_list.extend(type3)
queries_order = [""+str(x)+"" for x in class_list]
queries_order = ','.join(queries_order)

if 1:
    y_axis_list = ["roverhead", "overhead"]
    header = ["Relative \nOverhead %", "Overhead (ms)"]
    for idx, y_axis in enumerate(y_axis_list):
        print(data)
        p = ggplot(data, aes(x='qid', ymin=0, ymax=y_axis,  y=y_axis, color='system', fill='system', group='system', shape='overheadType'))
        p += geom_point(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.5, size=2)
        p += geom_linerange(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.8), width=0.8)
        p += axis_labels('Query', "{} (log)".format(header[idx]), "discrete", "log10", ykwargs=dict(breaks=[10, 100, 1000], labels=list(map(esc, ['10', '100', '1000']))))
        p += legend_side
        p += facet_grid(".~sf~qtype", scales=esc("free_x"), space=esc("free_x"))
        postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
        ggsave("figures/tpch_{}.png".format(y_axis), p, postfix=postfix,  width=12, height=6, scale=0.8)


if 0:
    queries_order = [""+str(x)+"" for x in range(1,23)]
    queries_order = ','.join(queries_order)
    p = ggplot(sd_data, aes(x='qid', y="size", fill="sf", group="sf"))
    p += geom_bar(stat=esc('identity'), alpha=0.8, position=position_dodge(width=0.6), width=0.5)
    p += axis_labels('Query', "Size (MB) [log]", "discrete", "log10") + coord_flip()
    p += legend_bottom
    postfix = """data$qid= factor(data$qid, levels=c({}))""".format(queries_order)
    ggsave("tpch_metrics.png", p, postfix=postfix,  width=2.5, height=4)

# summarize
if 1:
    q = f"""
    select lineage_type, sf,
    avg(exec_roverhead), avg(mat_roverhead), avg(roverhead),
    max(exec_roverhead), max(mat_roverhead), max(roverhead)
    from tpch_metrics
    where n_threads=1
    group by sf, lineage_type
    order by sf, lineage_type
    """
    out = con.execute(q).fetchdf()
    print(out)
