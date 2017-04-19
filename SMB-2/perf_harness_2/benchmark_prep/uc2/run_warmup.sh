source /perf_test/perf_harness_2/stac_benchmark/log.sh
CMD="./async_batch_multi_user.sh 1 60 2 30 120 hdfs://red21:48020/user/root/tpcds_sfactor_24 > /perf_test/perf_data_2/env_output.log 2>&1"
log "** Running warmup"

cd /perf_test/perf_harness_2

log "$CMD"
./async_batch_multi_user.sh 1 60 2 30 120 hdfs://red21:48020/user/root/tpcds_sfactor_24 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Warmup complete"
