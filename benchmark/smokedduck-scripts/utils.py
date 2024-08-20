from timeit import default_timer as timer
import random 
import sys
import pandas as pd
import numpy as np
import os.path
import json
from pygg import *

def relative_overhead(base, extra): # in %
    return max(((float(extra)-float(base))/float(base))*100, 0)

def overhead(base, extra): # in ms
    return max(((float(extra)-float(base)))*1000, 0)

def getAllExec(plan, op):
    """
    return execution time (sec) from profiling
    data stored in query plan
    """
    plan = plan.replace("'", "\"")
    plan = json.loads(plan)
    total = 0.0
    for k, v in plan.items():
        total += float(v)
    return total

def find_node_wprefix(prefix, plan):
    op_name = None
    if prefix[-4:] == "_mtm":
        prefix = prefix[:-4]
    for k, v in plan.items():
        if k[:len(prefix)] == prefix:
            op_name = k
            break
    return op_name

def getMat(plan, op):
    """
    return materialization time (sec) from
    profiling data stored in query plan
    """
    plan= plan.replace("'", "\"")
    plan = json.loads(plan)
    op = find_node_wprefix("CREATE_TABLE_AS", plan)
    if op == None:
        return 0
    return plan[op]


def get_op_timings(qid, op_name):
    plan_fname = '{}_plan.json'.format(qid)
    with open(plan_fname, 'r') as f:
        plan = json.load(f)
        print(plan)
        op_timings = get_op_timings_local(plan, op_name)
        print('X', op_timings)
    os.remove(plan_fname)
    return op_timings

def get_op_timings_local(plan, op_name_match):
    op_timings = None
    for c in  plan['children']:
        op_name = c['name'].strip()
        timing = c['timing']
        if op_name == op_name_match:
            return timing
        op_timings = get_op_timings_local(c, op_name_match)
    return op_timings

def gettimings(plan, res={}):
    for c in  plan['children']:
        op_name = c['name'].strip()
        timing = c['timing']
        res[op_name + str(len(res))] = timing
        gettimings(c, res)
    return res

def parse_plan_timings(qid):
    plan_fname = '{}_plan.json'.format(qid)
    plan_timings = {}
    with open(plan_fname, 'r') as f:
        plan = json.load(f)
        print(plan)
        plan_timings = gettimings(plan, {})
        print('X', plan_timings)
    os.remove(plan_fname)
    return plan_timings

def getStats(con, q):
    q_list = "select * from duckdb_queries_list() where query = ? order by query_id desc limit 1"
    print(q_list, q)
    query_info = con.execute(q_list, [q]).fetchdf()
    print(query_info)
    n = len(query_info)-1
    print("Query info: ", query_info.loc[n])
    query_id = query_info.loc[n, 'query_id']
    lineage_size = query_info.loc[n, 'size_mb']
    lineage_count = query_info.loc[n, 'tuples_count']
    nchunks = query_info.loc[n, 'nchunks']
    postprocess_time = query_info.loc[n, 'postprocess_time']
    plan = query_info.loc[n, 'plan']

    return lineage_size, lineage_count, nchunks, postprocess_time, plan

def execute(Q, con, args):
    Q = " ".join(Q.split())
    if args.lineage:
        con.execute("PRAGMA enable_lineage")
    if args.profile:
        con.execute("PRAGMA enable_profiling='json'")
        con.execute("PRAGMA profiling_output='{}_plan.json';".format(args.qid))
    if args.lineage and args.show_tables:
        con.execute("PRAGMA persist_lineage")
    start = timer()
    df = con.execute(Q).fetchdf()
    end = timer()
    if args.profile:
        con.execute("PRAGMA disable_profiling;")
    if args.lineage:
        con.execute("PRAGMA disable_lineage")
    return df, end - start

def DropLineageTables(con):
    tables = con.execute("PRAGMA show_tables").fetchdf()
    for index, row in tables.iterrows():
        if row["name"][:7] == "LINEAGE":
            con.execute("DROP TABLE "+row["name"])
    con.execute("PRAGMA clear_lineage")

def Run(q, args, con, table_name=None):
    dur_acc = 0.0
    print("Run: ", table_name, q)
    for j in range(args.repeat-1):
        df, duration = execute(q, con, args)
        dur_acc += duration
        if args.lineage and args.stats==False:
            DropLineageTables(con)
        if table_name:
            con.execute("drop table {}".format(table_name))
    
    df, duration = execute(q, con, args)
    dur_acc += duration
    if args.show_output:
        print(df)
    avg = dur_acc/args.repeat
    print("Avg Time in sec: ", avg, " output size: ", len(df)) 
    return avg, df

"""
z is an integer that follows a zipfian distribution
and v is a double that follows a uniform distribution
in [0, 100]. θ controls the zipfian skew, n is the table
size, and g specifies the number of distinct z values
"""

class ZipfanGenerator(object):
    def __init__(self, n_groups, zipf_constant, card):
        self.n_groups = n_groups
        self.zipf_constant = zipf_constant
        self.card = card
        self.initZipfan()

    def zeta(self, n, theta):
        ssum = 0.0
        for i in range(0, n):
            ssum += 1. / pow(i+1, theta)
        return ssum

    def initZipfan(self):
        zetan = 1./self.zeta(self.n_groups, self.zipf_constant)
        proba = [.0] * self.n_groups
        proba[0] = zetan
        for i in range(0, self.n_groups):
            proba[i] = proba[i-1] + zetan / pow(i+1., self.zipf_constant)
        self.proba = proba

    def nextVal(self):
        uni = random.uniform(0.1, 1.01)
        lower = 0
        for i, v in enumerate(self.proba):
            if v >= uni:
                break
            lower = i
        return lower

    def getAll(self):
        result = []
        for i in range(0, self.card):
            result.append(self.nextVal())
        return result

import random

def SelectivityGenerator(selectivity, card):
    card = card
    sel = selectivity
    n = int(float(card) * sel)
    print(card, sel, n)
    result = [0] * n
    for i in range(card-n):
        result.append(random.randint(1, card))

    random.shuffle(result)
    test_sel = [a for a in result if a == 0]
    print("execpted selectivity : ", sel, " actual: ", len(test_sel)/float(card))
    return result

def MicroDataZipfan(folder, groups, cardinality, max_val, a_list):
    for a in a_list:
        for g in groups:
            for card in cardinality:
                filename = folder+"zipfan_g"+str(g)+"_card"+str(card)+"_a"+str(a)+".csv"
                if not os.path.exists(filename):
                    print("generate file: ", filename)
                    if a == 0:
                        zipfan = [random.randint(0, g-1) for _ in range(card)]
                    else:
                        z = ZipfanGenerator(g, a, card)
                        zipfan = z.getAll()
                    unique_elements, counts = np.unique(zipfan, return_counts=True)
                    print(f"g={g}, card={card}, a={a}, len={len(zipfan)}")
                    print("1. ", len(unique_elements), unique_elements[:10], unique_elements[len(unique_elements)-10:])
                    print("2. ", counts[:10], counts[len(counts)-10:])
                    vals = np.random.uniform(0, max_val, card)
                    idx = list(range(0, card))
                    df = pd.DataFrame({'idx':idx, 'z': zipfan, 'v': vals})
                    df.to_csv(filename, index=False)

def MicroDataSelective(con, selectivity, cardinality, max_val=100):
    ## filter data
    for sel in selectivity:
        for card in cardinality:
            z_col = SelectivityGenerator(sel, card)
            vals = np.random.uniform(0, max_val, card)
            idx = list(range(0, card))
            df = pd.DataFrame({'idx': idx, 'z': z_col, 'v': vals})
            sel_str = f"{sel}".replace(".", "_")
            con.execute(f"""create table micro_table_{sel_str}_{card} as select * from df""")
            #print(con.execute(f"select * from micro_table_{sel_str}_{card} where z=0").df())

def MicroDataMcopies(folder, copies, cardinality, max_val):
    for m in copies:
        for card in cardinality:
            filename = folder+"m"+str(m)+"copies_card"+str(card)+".csv"
            if not os.path.exists(filename):
                data = []
                sequence = range(0, card)
                for i in range(m):
                    data.extend(sequence)
                vals = np.random.uniform(0, max_val, card*m)
                idx = list(range(0, card*m))
                df = pd.DataFrame({'idx':idx, 'z': data, 'v': vals})
                df.to_csv(filename, index=False)

# Source Sans Pro Light
legend = theme_bw() + theme(**{
  "legend.background": element_blank(), #element_rect(fill=esc("#f7f7f7")),
  "legend.justification":"c(1,0)", "legend.position":"c(1,0)",
  "legend.key" : element_blank(),
  "legend.title":element_blank(),
  "text": element_text(colour = "'#333333'", size=11, family = "'Arial'"),
  "axis.text": element_text(colour = "'#333333'", size=11),  
  "plot.background": element_blank(),
  "panel.border": element_rect(color=esc("#e0e0e0")),
  "strip.background": element_rect(fill=esc("#efefef"), color=esc("#e0e0e0")),
  "strip.text": element_text(color=esc("#333333"))
  
})
# need to add the following to ggsave call:
#    libs=['grid']
legend_bottom = legend + theme(**{
  "legend.position":esc("bottom"),
  #"legend.spacing": "unit(-.5, 'cm')"

})
legend_none = legend + theme(**{"legend.position": esc("none")})

legend_side = legend + theme(**{
  "legend.position":esc("right"),
})
