LOG_LOCATION="/perf_test/perf_harness_2/stac_benchmark/driver.log"

function log {
    echo "[$(date)] $*"
    echo "[$(date)] $*" >> $LOG_LOCATION
    }

#function log {
#    echo "[$(date)]: $*" | tee $LOG_LOCATION
#    }
