source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Running use case 1 benchmark"

CMD="./sync_interactive_multi_user.sh 20 120 30 1 5 hdfs://red21:48020/user/root/tpcds_sfactor_12 > /perf_test/perf_data_2/env_output.log 2>&1"
cd /perf_test/perf_harness_2

log "$CMD"
./sync_interactive_multi_user.sh 20 120 30 1 5 hdfs://red21:48020/user/root/tpcds_sfactor_12 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Use case 1 complete"
