source /perf_test/perf_harness_2/stac_benchmark/log.sh

log "** Starting HDFS"

source /opt/profile.yarn
log "`start-dfs.sh`"
sleep 45

log "`hadoop dfsadmin -report|head -13`"

log "** Finished starting HDFS"
