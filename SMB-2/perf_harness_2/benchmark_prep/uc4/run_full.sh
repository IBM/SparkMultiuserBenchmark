source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Running use case 4 benchmark"

CMD="./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1"
cd /perf_test/perf_harness_2

log "$CMD"
./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1

log "** Use case 4 complete"
