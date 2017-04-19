source /perf_test/perf_harness_2/stac_benchmark/log.sh

LASTRUN=`ls -d /perf_test/perf_data_2/*/ | grep 17 | tail -1`
DRIVER_LIST=`ls -1 $LASTRUN/driver_output_*`
DRIVER_NUM=`ls -1 $LASTRUN/driver_output_*|wc -l`
ITERATIONS=`wc -l $LASTRUN/Q*`
STREAM_TIMES=`cat $LASTRUN/env_output.log | grep "Starting Step"`
Q_PER_STREAM=`wc -l $LASTRUN/query*`

log "** Getting summary:"
log "RUN DIR:   $LASTRUN"
log "USERS RUN: $DRIVER_NUM"
log "ITERATIONS:"
log "$ITERATIONS"
log "QUERIES PER STREAM:"
log "$Q_PER_STREAM"
log "STREAM START TIMES:"
log "$STREAM_TIMES"

log "** Summary complete"
