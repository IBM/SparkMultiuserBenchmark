source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Cleaning up"

log "dropping caches.."
log "`for i in \`seq 15 25\`; do echo red$i; ssh red$i \"echo 3 > /proc/sys/vm/drop_caches\"; done`"

log "cleaning dirs.."
log "`/perf_test/perf_harness/clean_files_and_dirs_ALEX.sh`"

log "** Finished cleanup"
