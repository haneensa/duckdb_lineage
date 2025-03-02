#!/bin/bash

current_date=$(date +"%Y%m%d_%H%M")
#current_date=$(date +"%Y%m%d")
note="exp_$current_date"
db="micro_benchmark_$note.out"
repeat=1
mkdir figures

#rm filter_micro_db.out
#rm join_micro_db.out
#rm micro_agg_db.out
#rm micro_agg_db_v2.out
exps="--run_filter" #--run_filter --run_agg --run_hj --run_ineq --run_hj_mtn"

# iterate over each experiment
#python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_run.py --notes $note --repeat $repeat --save $exps
#python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_run.py --notes $note --repeat $repeat --save $exps --smoke
python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_run.py --notes $note --repeat $repeat  --save --lineage $exps --show_tables --show_output
#python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_run.py --notes $note --repeat $repeat --save --perm $exps

#python3 ~/smokedduck/benchmark/smokedduck-scripts/micro_plot.py --db $db $exps

echo $note
