import smokedduck
import pandas as pd

# Creating connection
with smokedduck.connect(':default:') as con:
    con.execute('CALL dbgen(sf=1);')

    skip_list = []
    # 2, 4
    # 13, 16, 17, 20
    for i in [1, 3, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 18, 19]:
        if i in skip_list:
            print(f"############# {i} SKIP ###########")
            continue
        con.execute("pragma enable_optimizer;")
        qid = str(i).zfill(2)
        query_file = f"benchmark/smokedduck-scripts/queries/q{qid}.sql"
        logical_file = f"benchmark/smokedduck-scripts/queries/perm_test/q{qid}.sql"
        try:
            with open(query_file, "r") as f:
                sql = " ".join(f.read().split())
            # Printing lineage that was captured from base query
            con.execute(sql, capture_lineage='lineage')
            lineage = con.lineage().df()
            #print(lineage)
            with open(logical_file, "r") as f:
                logical_sql = " ".join(f.read().split())

            con.execute("pragma enable_optimizer;")
            logical_lineage = con.execute(logical_sql).df()
            #print(logical_lineage)

            logical_lineage = logical_lineage.reindex(sorted(logical_lineage.columns), axis=1)
            lineage= lineage.reindex(sorted(lineage.columns), axis=1)

            logical_lineage = logical_lineage.sort_values(by=list(logical_lineage.columns)).reset_index(drop=True)
            lineage = lineage.sort_values(by=list(lineage.columns)).reset_index(drop=True)
            lineage = lineage[list(logical_lineage.columns)]
            #logical_lineage= logical_lineage.astype(lineage.dtypes)
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
        except:
             print(f"############# {qid} FAILED ###########")
           

