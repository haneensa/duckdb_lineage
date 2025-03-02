from itertools import product
import json
import pandas as pd
import argparse
from pygg import *
import duckdb
from duckdb.typing import *
from utils import legend_bottom, legend_none, legend_side, relative_overhead, overhead, getAllExec, getMat, get_op_timings


parser = argparse.ArgumentParser(description='TPCH benchmarking script')
parser.add_argument('--db', type=str,  help="exp notes", default="")
parser.add_argument('--run_filter', action='store_true',  help="eval filter")
parser.add_argument('--run_ineq', action='store_true',  help="eval ineq join")
parser.add_argument('--run_hj', action='store_true',  help="eval hash join fkpk")
parser.add_argument('--run_hj_mtn', action='store_true',  help="eval hash join mtn")
parser.add_argument('--run_agg', action='store_true',  help="eval agg")
args = parser.parse_args()

plot_filter = args.run_filter
plot_join = args.run_hj
plot_ineq = args.run_ineq
plot_join_mtn = args.run_hj_mtn
plot_agg = args.run_agg

dbname = args.db
con = duckdb.connect(dbname)

con.create_function("getMat", getMat, [VARCHAR], FLOAT)
con.create_function("getAllExec", getAllExec, [VARCHAR], FLOAT)
con.create_function("get_op_timings", get_op_timings, [VARCHAR, VARCHAR], FLOAT)

# 1) separate materialize overhead from propagation overhead for Logical
# 2) separate accumelating lineage vs copying overhead for SmokedDuck

metrics = ["runtime", "output_size", "lineage_size_mb", "lineage_count", "nchunks",
            "postprocess",'op_t', 'all_t', 'mat_t', 'exec_t']
m = ','.join(metrics)

avg_metrics = """
max(output_size) as output_size,
max(lineage_size_mb) as lineage_size_mb,
max(lineage_count) as lineage_count,
max(nchunks) as nchunks,
max(postprocess) as postprocess,
avg(runtime) as runtime"""

avg_overhead = """
avg(getAllExec(plan_timings)) as all_t,
avg(getMat(plan_timings)) as mat_t,
avg(getAllExec(plan_timings) - getMat(plan_timings)) as exec_t
"""

aug_baseline = """
t1.all_t as base_all_t,
t1.runtime as base_runtime,
t1.output_size as base_output, 
t1.op_t as base_op_t, 
t1.mat_t as base_mat_t,
t1.exec_t as base_exec_t,
t2.* 
"""

perm_overheads = """
(exec_t-base_exec_t)*1000 as exec_overhead,
((exec_t-base_exec_t)/base_exec_t)*100 as exec_roverhead,

(mat_t - base_mat_t)*1000 as mat_overhead,
((mat_t - base_mat_t) / base_exec_t) *100 as mat_roverhead,

(all_t-base_all_t)*1000 as all_overhead,
((all_t-base_all_t)/base_exec_t)*100 as all_roverhead,

(op_t-base_op_t)*1000 as op_overhead,
((op_t-base_op_t)/base_op_t)*100 as op_roverhead,
"""

sd_overheads = """
(exec_t-base_exec_t)*1000 as exec_overhead,
((exec_t-base_exec_t)/base_exec_t)*100 as exec_roverhead,

0 as mat_overhead,
0 as mat_roverhead,

(exec_t-base_exec_t)*1000 as all_overhead,
((exec_t-base_exec_t)/base_exec_t)*100 as all_roverhead,

(op_t-base_op_t)*1000 as op_overhead,
((op_t-base_op_t)/base_op_t)*100 as op_roverhead,
"""

def mktemplate(overheadType, prefix, table, header):
    return f"""
    SELECT '{overheadType}' as overheadtype, {header}, lineage_type as system,
           greatest(0, {prefix}overhead) as overhead, greatest(0, {prefix}roverhead) as roverhead
    FROM {table}"""

template = f"""
  WITH data as (
    {mktemplate('Total', 'all_', 'mdata', {})}
    UNION ALL
    {mktemplate('Materialize', 'mat_', 'mdata', {})}
    UNION ALL
    {mktemplate('Execute', 'exec_', 'mdata', {})}
  ) SELECT * FROM data {"{}"} ORDER BY overheadType desc """

cond = " and system<>'Baseline'"

def PlotLines(pdata, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, wrap=None, xkwargs=None, labeller=None):
    print("Plot:")
    print(pdata)
    if linetype:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color, linetype=linetype, shape=linetype))
        p += geom_point()
    else:
        p = ggplot(pdata, aes(x=x_axis, y=y_axis, color=color))
    
    p +=  geom_line()
    if xkwargs:
        p += axis_labels(x_label, y_label, x_type, y_type, xkwargs=xkwargs)
    else:
        p += axis_labels(x_label, y_label, x_type, y_type)

    if facet:
        if labeller:
            p += facet_grid(facet, scales=esc("free_y"), labeller=labeller)
        else:
            p += facet_grid(facet, scales=esc("free_y"))

    if wrap:
        p += facet_wrap(wrap)
    p += legend_bottom
    p += legend_side
    ggsave("figures/"+fname, p,  width=w, height=h, scale=0.8)

y_axis_list = ["roverhead", "overhead"]
y_header = ["Relative\nOverhead %", "Overhead (ms)"]
linetype = "overheadtype"

################### Filter ###############################
if plot_filter:
    filter_data = con.execute("select * from filter_results").df()
    print(filter_data)
    print(filter_data.columns.tolist())

    header_unique = ["lineage_type", "op_name", "card", "sel"]
    g = ','.join(header_unique)

    # 1) average over iterations
    filter_data_avg = con.execute(f"""select {g}, {avg_metrics}, {avg_overhead},
                                avg(get_op_timings(plan_timings, op_name)) as op_t
                                from filter_data
                                group by {g}""").fetchdf()
    print(filter_data_avg)
    header_unique.remove("lineage_type")
    g = ','.join(header_unique)
    wbaseline = con.execute(f"""select {aug_baseline}
                      from (select {g}, {m} from filter_data_avg where lineage_type='Baseline') as t1
                      join filter_data_avg as t2 using ({g})
                      """).df()
    print(wbaseline)
    mdata = con.execute(f"""select {g}, lineage_type, {m}, {perm_overheads}
                      from wbaseline where lineage_type<>'SmokedDuck'
                      UNION ALL
                      select {g}, lineage_type, {m}, {sd_overheads}
                      from wbaseline where lineage_type='SmokedDuck'
                      """).df()
    where = f"WHERE op_name IN ('SEQ_SCAN','FILTER') {cond}"
    data  = con.execute(template.format(g, g, g, where)).fetchdf()
    data = con.execute("""select 'SD' as sys_label, * from data where system='SmokedDuck' UNION ALL 
    SELECT 'Logical' as sys_label, * from data where system='Perm' """).df()
    data['op_label'] = data['op_name'].apply(lambda x: "Filter Scan" if x=="SEQ_SCAN" else "Filter")
    print(data)
    sample_data = con.execute("select * from data where overheadType<>'Execute'").df()
    sample_data_card = con.execute("select * from data where overheadType<>'Execute' and card=10000000").df()
    xkwargs=dict(labels=list(map(esc, ['0', '.25', '.5', '.75', '1'])))
    # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Sel", "sys_label", "op_label~card"
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_filter.png".format(y_axis), 12, 6
        PlotLines(sample_data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
        
        fname, w, h = "micro_all_{}_line_filter.png".format(y_axis), 12, 6
        PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)

        x_axis, x_label, color, facet = "sel", "Sel", "sys_label", "~op_label"
        x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_filter_10M.png".format(y_axis), 6.5, 2.5
        PlotLines(sample_data_card, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, xkwargs)

    # write summary: 
    # metrics: relative overhead, overhead, Speedup (Logical/SD)
    # avg, avg per n, avg per sel
    overheadTypes = ["Total", "Materialize", "Execute"]
    op_names = ["SEQ_SCAN", "FILTER"]
    for op_name, overheadType in product(op_names, overheadTypes):
        scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
        print(scond)
        summary = con.execute(f"""select overheadtype, system, op_name, sel,
                avg(overhead), max(overhead), min(overhead),
                avg(roverhead), max(roverhead), min(roverhead)
                from data {scond} group by overheadtype, system, op_name,  sel
                order by overheadtype, system, op_name, sel
                """).df()
        print(summary)
        
        summary = con.execute(f"""select overheadtype, system, op_name,
                avg(overhead), max(overhead), min(overhead),
                avg(roverhead), max(roverhead), min(roverhead)
                from data {scond} group by overheadtype, system, op_name
                order by overheadtype, system, op_name
                """).df()
        print(summary)
        # TODO: add speedup

    summary = con.execute(f"""select overheadtype, system, 
            avg(overhead), max(overhead), min(overhead),
            avg(roverhead), max(roverhead), min(roverhead)
            from data group by overheadtype, system
            order by overheadtype, system
            """).df()
    print(summary)



################### Join ###############################
if plot_join or plot_ineq or plot_join_mtn:
    join_data = con.execute("select * from join_results").df()
    print(join_data)
    print(join_data.columns.tolist())

    header_unique = ["lineage_type", "op_name", "n1", "n2", "sel", "skew", "ncol", "groups"]
    g = ','.join(header_unique)

    # 1) average over iterations
    join_data_avg = con.execute(f"""select {g}, {avg_metrics}, {avg_overhead},
                                avg(get_op_timings(plan_timings, op_name)) as op_t
                                from join_data
                                group by {g}""").fetchdf()
    print(join_data_avg)
    header_unique.remove("lineage_type")
    g = ','.join(header_unique)
    wbaseline = con.execute(f"""select {aug_baseline}
                      from (select {g}, {m} from join_data_avg where lineage_type='Baseline') as t1
                      join join_data_avg as t2 using ({g})
                      """).df()
    print(wbaseline)
    mdata = con.execute(f"""select {g}, lineage_type, {m}, {perm_overheads}
                      from wbaseline where lineage_type<>'SmokedDuck'
                      UNION ALL
                      select {g}, lineage_type, {m}, {sd_overheads}
                      from wbaseline where lineage_type='SmokedDuck'
                      """).df()
    print(mdata)
    
if plot_join:
    op_names = ["HASH_JOIN", "HASH_JOINvarchar_"]
    for op_name in op_names:
        where = f"WHERE op_name IN ('{op_name}') {cond}"
        data  = con.execute(template.format(g, g, g, where)).fetchdf()
        sample_data = con.execute("select * from data where overheadType<>'Execute'").df()
        print(sample_data)
        # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "groups", "Groups", "system", "~skew~n1"
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = f"micro_{y_axis}_line_{op_name}.png", 8, 6
            PlotLines(sample_data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
            
        overheadTypes = ["Total", "Materialize", "Execute"]
        for overheadType in overheadTypes:
            scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
            print(scond)
            summary = con.execute(f"""select overheadtype, system, op_name, n1, n2,
                    avg(overhead), max(overhead), min(overhead),
                    avg(roverhead), max(roverhead), min(roverhead)
                    from data {scond} group by overheadtype, system, op_name,  n1, n2
                    order by overheadtype, system, op_name, n1, n2
                    """).df()
            print(summary)
    
if plot_ineq:
    # plot |T1| = 1K and |T2| = 1M
    where = f"WHERE overheadType<>'Execute' and op_name<>'CROSS_PRODUCT' and n2=1000000 {cond}"
    data  = con.execute(template.format(g, g, g, where)).fetchdf()
    d = { "BLOCKWISE_NL_JOIN": "BNL", "PIECEWISE_MERGE_JOIN": "Merge", "NESTED_LOOP_JOIN": "NL"}
    data['op_label'] = data['op_name'].apply(d.get)
    system_d = { "SmokedDuck": "SD", "Perm": "Logical"}
    data['sys_label'] = data['system'].apply(system_d.get)
    print(data)
    # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "sel", "Sel", "sys_label", "~op_label"
        x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
        fname, w, h = f"micro_{y_axis}_line_1M_1k_ineq.png", 7, 2.5
        PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)

    op_names = ["NESTED_LOOP_JOIN", "PIECEWISE_MERGE_JOIN", "BLOCKWISE_NL_JOIN"]
    for op_name in op_names:
        where = f"WHERE op_name IN ('{op_name}') {cond}"
        data  = con.execute(template.format(g, g, g, where)).fetchdf()
        print(data)
        # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "sel", "Sel", "system", "~n2"
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = f"micro_{y_axis}_line_{op_name}.png", 8, 6
            PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
    
        overheadTypes = ["Total", "Materialize", "Execute"]
        for overheadType in overheadTypes:
            scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
            print(scond)
            summary = con.execute(f"""select overheadtype, system, op_name, n1,  sel,
                    avg(overhead), max(overhead), min(overhead),
                    avg(roverhead), max(roverhead), min(roverhead)
                    from data {scond} group by overheadtype, system, op_name,  n1, sel
                    order by overheadtype, system, op_name, n1, sel
                    """).df()
            print(summary)
    
    op_names = ["CROSS_PRODUCT"]
    for op_name in op_names:
        where = f"WHERE op_name IN ('{op_name}') {cond}"
        data  = con.execute(template.format(g, g, g, where)).fetchdf()
        print(data)
        # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "n2", "N2", "system", None
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = f"micro_{y_axis}_line_{op_name}.png", 8, 6
            PlotLines(data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
    
        overheadTypes = ["Total", "Materialize", "Execute"]
        for overheadType in overheadTypes:
            scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
            print(scond)
            summary = con.execute(f"""select overheadtype, system, op_name, n1, n2,
                    avg(overhead), max(overhead), min(overhead),
                    avg(roverhead), max(roverhead), min(roverhead)
                    from data {scond} group by overheadtype, system, op_name,  n1, n2
                    order by overheadtype, system, op_name, n1, n2
                    """).df()
            print(summary)

if plot_join_mtn:
    op_names = ["HASH_JOIN_mtm", "HASH_JOIN_mtmvarchar_"]
    for op_name in op_names:
        where = f"WHERE op_name IN ('{op_name}') {cond}"
        data  = con.execute(template.format(g, g, g, where)).fetchdf()
        sample_data = con.execute("select * from data where overheadType<>'Execute'").df()

        sample_data_1m = con.execute("select * from sample_data where n2=1000000 and skew=1").df()
        sample_data_1m['skew'] = sample_data_1m['skew'].apply(lambda s: f"Skew: {s}")
        print("--->")
        print(sample_data_1m)
        print("--->")
        print(data)
        # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
        for idx, y_axis in enumerate(y_axis_list):
            x_axis, x_label, color, facet = "groups", "Groups", "system", "~skew~n1"
            #x_type, y_type, y_label = "continuous",  "continueous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "continuous",  "log10", "{} [log]".format(y_header[idx])
            fname, w, h = f"micro_{y_axis}_line_{op_name}.png", 8, 6
            PlotLines(sample_data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
        
            x_axis, x_label, color, facet = "groups", "Groups", "system", "~skew"
            #x_type, y_type, y_label = "continuous",  "continuous", "{}".format(y_header[idx])
            x_type, y_type, y_label = "log10",  "continuous", "{}".format(y_header[idx])
            fname, w, h = f"micro_{y_axis}_line_1M_{op_name}.png", 6, 2.75
            PlotLines(sample_data_1m,  x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h)
            
    
    
        overheadTypes = ["Total", "Materialize", "Execute"]
        for overheadType in overheadTypes:
            scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
            print(scond)
            summary = con.execute(f"""select overheadtype, system, op_name, n1, n2, groups,
                    avg(overhead), max(overhead), min(overhead),
                    avg(roverhead), max(roverhead), min(roverhead)
                    from data {scond} group by overheadtype, system, op_name,  n1, n2, groups
                    order by overheadtype, system, op_name, n1, n2,groups
                    """).df()
            print(summary)


################### Aggregation ###############################
if plot_agg:
    agg_data = con.execute("select * from agg_results").df()
    print(agg_data)
    print(agg_data.columns.tolist())

    header_unique = ["lineage_type", "op_name", "card", "groups", "col"]
    g = ','.join(header_unique)

    # 1) average over iterations
    agg_data_avg = con.execute(f"""select {g}, {avg_metrics}, {avg_overhead},
                                0 as op_t,
                                from agg_data
                                group by {g}""").fetchdf()
    print(agg_data_avg)
    header_unique.remove("lineage_type")
    g = ','.join(header_unique)
    wbaseline = con.execute(f"""select {aug_baseline}
                      from (select {g}, {m} from agg_data_avg where lineage_type='Baseline') as t1
                      join agg_data_avg as t2 using ({g})
                      """).df()
    print(wbaseline)
    mdata = con.execute(f"""select {g}, lineage_type, {m}, {perm_overheads}
                      from wbaseline where lineage_type<>'SmokedDuck'
                      UNION ALL
                      select {g}, lineage_type, {m}, {sd_overheads}
                      from wbaseline where lineage_type='SmokedDuck'
                      """).df()
    print(mdata)
    where = f"where system IN ('Baseline', 'SmokedDuck', 'Perm', 'Perm_list', 'Smoke')  {cond}"
    data  = con.execute(template.format(g, g, g, where)).fetchdf()
    sample_data = con.execute("select * from data where overheadType<>'Execute'").df()
    print(con.execute("select * from data where system='Perm'").df())

    sample_data_10m = con.execute("select * from sample_data where card=10000000 and op_name<>'HASH_GROUP_BY' and system<>'Perm_list'").df()
    d = { "PERFECT_HASH_GROUP_BY": "PERFECT HASH GROUP BY", "HASH_GROUP_BY_var": "HASH GROUP BY", "HASH_GROUP_BY": "HASH GROUP BY"}
    sample_data_10m['op_label'] = sample_data_10m['op_name'].apply(d.get)
    sample_data_10m['card'] = sample_data_10m['card'].apply(lambda v: v / 1000000)
    # 1. x-axis: selectivity, y-axis: runtime, facet: cardinality
    system_d = { "SmokedDuck": "This work", "Perm": "Logical", "Smoke": "Smoke"}
    sample_data_10m['sys_label'] = sample_data_10m['system'].apply(system_d.get)
    sample_data_10m_10 = con.execute("select * from sample_data_10m where groups=10 and op_name='PERFECT_HASH_GROUP_BY' and overheadtype='Total'").df()
    print(sample_data_10m_10)
    for idx, y_axis in enumerate(y_axis_list):
        x_axis, x_label, color, facet = "groups", "Groups (g)", "system", "~op_name~card"
        #x_type, y_type, y_label = "continuous",  "continueous", "{}".format(y_header[idx])
        x_type, y_type, y_label = "log10", "log10", "{} [log]".format(y_header[idx])
        fname, w, h = "micro_{}_line_agg.png".format(y_axis), 6, 3
        PlotLines(sample_data, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, None)
        
        x_axis, x_label, color, facet = "groups", "Groups (g)", "sys_label", "~card~op_label"
        labeller="labeller(card=function(x)paste('# Tuples:',x,'M',sep=''))"
        x_type, y_type, y_label = "log10", "log10", "{} [log]".format(y_header[idx])
        xkwargs=None # dict(breaks=[10,100,1000], labels=list(map(esc,['10','100','1000'])))
        fname, w, h = "micro_{}_10M_line_reg_agg.png".format(y_axis), 8, 2.5
        PlotLines(sample_data_10m, x_axis, y_axis, x_label, y_label, x_type, y_type, color, linetype, facet, fname, w, h, None, xkwargs, labeller)

        # plot 10M, 10 groups, Smoke, SD, Logical
        x_axis, x_label, color, facet = "sys_label", "", "overheadtype", None
        labeller="labeller(card=function(x)paste('# Tuples:',x,'M',sep=''))"
        x_type, y_type, y_label = "discrete", "continuous", "{}".format(y_header[idx])
        xkwargs=None # dict(breaks=[10,100,1000], labels=list(map(esc,['10','100','1000'])))
        fname, w, h = "micro_baselines_{}_10M_10_line_reg_agg.png".format(y_axis), 4, 2.5
        
        p = ggplot(sample_data_10m_10, aes(x=x_axis, y=y_axis, fill=color, group=color))
        p += axis_labels(x_label, y_label, x_type, y_type)
        p += geom_bar(stat=esc('identity'), alpha=0.8, posiion=position_dodge(width=0.1), width=0.8)
        p += legend_none
        ggsave("figures/"+fname, p,  width=w, height=h, scale=0.8)
        
    overheadTypes = ["Total", "Materialize", "Execute"]
    op_names = ["HASH_GROUP_BY", "HASH_GROUP_BY_var", "PERFECT_HASH_GROUP_BY"]
    for op_name, overheadType in product(op_names, overheadTypes):
        scond = f"where op_name='{op_name}' and overheadtype='{overheadType}'"
        print(scond)
        
        summary = con.execute(f"""select overheadtype, system, op_name, card, groups,
                avg(overhead), max(overhead), min(overhead),
                avg(roverhead), max(roverhead), min(roverhead)
                from data {scond} group by overheadtype, system, op_name, card, groups
                order by overheadtype, system, op_name, card, groups
                """).df()
        print(summary)
