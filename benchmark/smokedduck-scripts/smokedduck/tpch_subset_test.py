import numpy as np
import smokedduck
import pandas as pd

#pd.set_option('display.max_rows', None)


# Creating connection
with smokedduck.connect(':default:') as con:
    con.execute('CALL dbgen(sf=0.1);')
    con.execute("pragma threads=1")

    #  streaming_limit
    skip_list = []
    # 16 (column_data_scan, mark), 21 (right, semi, anti), 22 (right_delim_join, mark, right_anti), 4 (right_delim_join, right_semi)
    # 20 (right_delim_join, right), 17 (right_delim_join, right), 2 (right_delim_join, right),
    for i in [17]: #1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18, 19]:
        if i in skip_list:
            print(f"############# {i} SKIP ###########")
            continue
        print(f"############# Eval {i} ###########")
        con.execute("pragma enable_optimizer;")
        qid = str(i).zfill(2)
        query_file = f"benchmark/smokedduck-scripts/queries/q{qid}.sql"
        logical_file = f"benchmark/smokedduck-scripts/queries/perm_test/q{qid}.sql"
        #try:
        with open(query_file, "r") as f:
            sql = " ".join(f.read().split())
        # Printing lineage that was captured from base query
        #con.execute(f"PRAGMA set_agg('reg')")
        out = con.execute(sql, capture_lineage='lineage').df()
        print(out)
        print("start lineage query")
        #print(con.execute("select * from LINEAGE_2_ORDER_BY_11").df())
        #print(con.execute("select * from LINEAGE_2_HASH_JOIN_13").df())
        #print(con.execute("select * from lineage_2_hash_group_by_6 order by out_index").df())
        #print(con.execute("select * from lineage_2_hash_group_by_10 order by out_index").df())
        #print(con.execute("select * from lineage_2_SEQ_SCAN_0 order by in_index").df())
        #con.execute("pragma disable_optimizer")
        lineage = con.lineage().df()
        print(lineage.columns)
        lineage = lineage.sort_values(by=list(["out_index"])).reset_index(drop=True)
        print(lineage)


        assert False
        with open(logical_file, "r") as f:
            logical_sql = " ".join(f.read().split())

        print("start logical", logical_sql)
        logical_lineage = con.execute(logical_sql).df()
        logical_lineage = logical_lineage.sort_values(by=list(["out_index"])).reset_index(drop=True)

        
        cols = list(lineage.columns)
        out_index_index = cols.index('out_index')
        y_value = cols.pop(out_index_index)
        cols.insert(0, y_value)
        print("sort by : ", cols)
        logical_lineage = logical_lineage.reindex(cols, axis=1)
        lineage= lineage.reindex(cols, axis=1)

        logical_lineage = logical_lineage.sort_values(by=cols).reset_index(drop=True)
        lineage = lineage.sort_values(by=cols).reset_index(drop=True)
        lineage = lineage[cols]
        #logical_lineage= logical_lineage.astype(lineage.dtypes)
        assert False
        print("======Logical=======")
        print(logical_lineage)
        print("======SD=======")
        print(lineage)
        df_all = logical_lineage.merge(lineage, on=list(logical_lineage.columns), how='left', indicator=True)
        right_only = df_all[ df_all['_merge'] == "right_only"]
        left_only = df_all[ df_all['_merge'] == "left_only"]
        both = df_all[ df_all['_merge'] == "both"]
        if len(left_only) > 0:
            print(right_only)
            print(left_only)
            print(both)
        assert (len(both) == len(lineage) and len(right_only) == 0) or lineage.equals(logical_lineage), f"DataFrames do not have equal content, {qid}"
        print(f"############# {qid} PASSED ###########")
        #except:
        #     print(f"############# {qid} FAILED ###########")
           

