source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Starting CwS"
source /opt/profile.conductor
ENV_INFO=`cat /opt/profile.conductor`

log "$ENV_INFO"
log "`yes | egosh ego start all`"
sleep 120

log "`egosh user logon -u Admin -x Admin`"
log "`egosh service stop all`"
sleep 10

log "`egosh service start spark201-sparkss spark201-sparkhs spark201-sparkms-batch`"
sleep 75
log "`egosh rg`"

log "** Finished starting CwS"
