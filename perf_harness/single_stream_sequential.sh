if [ "$#" -ne "6" ]
then
        echo "Usage:"
        echo "[NUMBER OF PARTITIONS]"
        echo "[INPUT DIRECTORY OF THE SORT FILES]"
        echo "[OUTPUT DIRECTORY OF THE SORT FILES]"
        echo "[MODE/IP ADDRESS OF MASTER (example: local[*]) ]"
        echo "[NUMBER OF TIMES TO RUN SORT]"
        echo "[STEP ID]"
        exit 1
fi

NUMPARTITIONS=$1
INPUTDIR=$2
OUTPUTDIR=$3
MASTER=$4
NUMSORT=$5
STEP_ID=$6

MASTER_NAME=`cat /perf_test/perf_harness/master_list`
CURRENTTIME=$(date "+%Y%m%d-%H%M")
PERFDIR=/perf_test/perf_data
FILENAME=$PERFDIR/single-stream-results_$CURRENTTIME$STEP_ID.txt
JOBLOG=$PERFDIR/single-stream-$STEP_ID-job.log

#changes made
OUTPUT=/opt/Output/output-20000000-records-40-parts

#fetch timestamp
TIMESTAMP_FILE=/tmp/timestamp.run
TIMESTAMP=$(cat $TIMESTAMP_FILE)

COUNTER=0
exec 3>&1 4>&2
while [ $COUNTER -lt $NUMSORT ]; do
    echo "Starting run $COUNTER"
    echo "Starting run $COUNTER" >> $FILENAME
    time=$(date +%R:%S)
    label=": Step ${STEP_ID}, Iteration ${COUNTER}"
    (echo "$time Timing info for run $COUNTER: "$( { time ./sort.sh $NUMPARTITIONS $INPUTDIR $OUTPUTDIR$COUNTER'_'$STEP_ID $MASTER $JOBLOG "$label" 1>&3 2>&4; } 2>&1 ) >> $FILENAME )
    #(echo "$time Timing info for run $COUNTER: "$( { time /opt/spark-bench/Terasort/bin/run.sh "$label" 1>&3 2>&4; } 2>&1 ) >> $FILENAME )
    #echo "$TIMESTAMP.$STEP_ID $COUNTER `date +%s`" | nc $MASTER_NAME 2003
    #echo "$TIMESTAMP.$STEP_ID.$COUNTER 1 `date +%s`" | nc $MASTER_NAME 2003
    echo "timing.$TIMESTAMP.$STEP_ID.$COUNTER 1 `date +%s`" | nc $MASTER_NAME 2003
    /perf_test/perf_utilities/clean_sort_output.sh $OUTPUT${COUNTER}_$STEP_ID
    let COUNTER=COUNTER+1
    sleep 1
done

wait
exec 3>&- 4>&-

echo "See $JOBDIR/$FILENAME for the timing results of all $NUMSORT runs."
