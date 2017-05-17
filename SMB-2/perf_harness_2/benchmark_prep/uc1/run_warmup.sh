source /perf_test/perf_harness_2/stac_benchmark/log.sh
CMD="sync_interactive_multi_user.sh 3 120 5 1 5 hdfs://<YOUR_HDFS_HOST>:48020/user/root/tpcds_sfactor_12 > /perf_test/perf_data_2/env_output.log 2>&1"
log "** Running warmup"

cd /perf_test/perf_harness_2

log "$CMD"
./sync_interactive_multi_user.sh 3 120 5 1 5 hdfs://<YOUR_HDFS_HOST>:48020/user/root/tpcds_sfactor_12 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Warmup complete"
