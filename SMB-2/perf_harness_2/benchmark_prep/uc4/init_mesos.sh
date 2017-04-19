source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Starting MESOS"
source /opt/profile.mesos101
ENV_INFO=`cat /opt/profile.mesos101`

log "$ENV_INFO"
log "`sh /opt/scripts/start_mesos_uc4.sh`"
sleep 60

log "** Finished starting Mesos"
