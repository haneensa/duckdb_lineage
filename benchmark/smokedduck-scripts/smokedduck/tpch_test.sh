python3 smokedduck/prep_db.py  --specs "lineitem.l_linestatus|lineitem.l_tax"
python3 smokedduck/whatif_demo.py --specs "lineitem.l_linestatus" --groups 0 --aggid 0 --load_sql_from_file 1 --sql_file "queries/tpch/tpch_01.sql"
