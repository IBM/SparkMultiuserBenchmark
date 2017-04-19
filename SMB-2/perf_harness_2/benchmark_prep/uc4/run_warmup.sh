source /perf_test/perf_harness_2/stac_benchmark/log.sh
CMD="./mixed_multi_tenant.sh 2 2 60 1 1 > /perf_test/perf_data_2/env_output.log 2>&1"
log "** Running warmup"

cd /perf_test/perf_harness_2

log "$CMD"
./mixed_multi_tenant.sh 2 2 60 1 1 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Warmup complete"
