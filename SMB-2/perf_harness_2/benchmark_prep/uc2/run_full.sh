source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Running use case 2 benchmark"

CMD="./async_batch_multi_user.sh 5 60 20 30 120 hdfs://red21:48020/user/root/tpcds_sfactor_24 > /perf_test/perf_data_2/env_output.log 2>&1"
cd /perf_test/perf_harness_2

log "$CMD"
./async_batch_multi_user.sh 5 60 20 30 120 hdfs://red21:48020/user/root/tpcds_sfactor_24 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Use case 2 complete"
