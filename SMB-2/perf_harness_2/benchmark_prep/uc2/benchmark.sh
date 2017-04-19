source /perf_test/perf_harness_2/stac_benchmark/uc2/log.sh

echo "DID YOU SOURCE THE CORRECT ENV?"
echo "DID YOU SET MASTER_LIST?"
sleep 5

log "******************"
log "Starting benchmark"
log "******************"

PLATFORM=$arg1

# Start hdfs
sh init_hdfs.sh
sleep 10

# Init PLATFORM
#sh init_cws.sh
#sh init_yarn.sh
sh init_mesos.sh
sleep 10

# Start warmup
sh run_warmup.sh
sleep 60

# Get summary of warmup
sh summary.sh

# Cleanup warmup
sh cleanup.sh

# Start full run
sh run_full.sh
sleep 300

# Get summary of full run
sh summary.sh

# Start cleanup
sh cleanup.sh

log "******************"
log "Finished benchmark"
log "******************"
