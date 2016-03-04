if [ "$#" -ne "4" ]
then
        echo "Usage:"
        echo "[NUMBER OF STEPS]"
        echo "[STEP OFFSET]"
        echo "[NUMBER OF ITERATIONS FOR EACH STEP]"
        echo "[MODE/IP ADDRESS OF THE MASTER]"
        exit 1
fi

NUM_STEPS=$1
STEP_OFFSET=$2
STEP_ITERATIONS=$3
MASTER=$4

NUMPARTITIONS=40

#Point to the input file on hdfs
INPUTDIR=file:///opt/Input/input-20000000-records-40-parts

#I would keep this setting, so that we /perf_utilities/clean_sort_output.sh can clean this file
OUTPUTDIR=file:///opt/Output/output-20000000-records-40-parts

#mkdir /tmp/spark-events
#chmod -R 777 /tmp/spark-events

DATEFORNMON=$(./start_nmon_monitor.sh)

PERF_DATA=/perf_test/perf_data

echo "DATEFORNMON=[$DATEFORNMON]"
echo $DATEFORNMON > /tmp/timestamp.run
echo $DATEFORNMON > /perf_test/perf_data/steps-$NUM_STEPS--sleep-$STEP_OFFSET--iters-$STEP_ITERATIONS.txt


COUNTER=0
while [ $COUNTER -lt $NUM_STEPS ]; do
    echo "Starting Step : $COUNTER"
    ./single_stream_sequential.sh $NUMPARTITIONS $INPUTDIR $OUTPUTDIR $MASTER $STEP_ITERATIONS $COUNTER &
    let COUNTER=COUNTER+1
    sleep $STEP_OFFSET
done

wait

#<<COMMENT_OUT_NMON
./stop_nmon_monitor.sh $DATEFORNMON
echo "nmon data stored with timestamp of $DATEFORNMON"

echo "cleaning output dir $OUTPUTDIR ... "
rm -rf $OUTPUTDIR*
#hadoop fs -rm -r $OUTPUTDIR*
echo "cleaning output dir $OUTPUTDIR ... Done!"

# below collecting logs nmon and creating processed timing data
cd $PERF_DATA
mkdir $DATEFORNMON #dir should exist since we call stop_nmon_monitor.sh
chmod -R 777 $DATEFORNMON
sudo mv single-stream-results_*.txt  *.tar.gz single-stream-*-job.log $DATEFORNMON/
sudo mv steps-*--sleep-*--iters-*.txt $DATEFORNMON/
/perf_test/perf_utilities/process-timings.sh /perf_test/perf_data/$DATEFORNMON #process timings
echo "steps=[$NUM_STEPS] sleep=[$STEP_OFFSET] iters=[$STEP_ITERATIONS]"
echo "nmon data stored and processed timings stored in /perf_test/perf_data/$DATEFORNMON"
#COMMENT_OUT_NMON

echo "Step-up complete!"
