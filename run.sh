python3 benchmark/smokedduck/tpch_capture.py may27_g  --profile --save_csv --sf 10
python3 benchmark/smokedduck/tpch_capture.py may27_g  --profile --save_csv --csv_append --sf 10 --lineage
python3 benchmark/smokedduck/tpch_capture.py may27_g  --profile --save_csv --csv_append --sf 10 --perm
python3 benchmark/smokedduck/tpch_capture.py may27_g  --profile --save_csv --csv_append --sf 10 --perm --opt
python3 benchmark/smokedduck/tpch_capture.py may27_g  --profile --save_csv --csv_append --sf 10 --gprom
python3 benchmark/smokedduck/tpch_plot.py --file tpch_benchmark_capture_may27_g.csv
