source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Starting YARN"
source /opt/profile.yarn
ENV_INFO=`cat /opt/profile.yarn`

log "$ENV_INFO"
log "`start-yarn.sh`"
sleep 60

log "** Finished starting YARN"
