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
#INPUTDIR=file:///users/mesos/perf_harness/input-20000000-records-40-parts
#INPUTDIR=$HDFS_URL/SparkBench/Terasort/Input
#INPUTDIR=$HDFS_URL/SparkBench/Terasort/Input/input-20000000-records-40-parts
INPUTDIR=hdfs://red21:48020/SparkBench/Terasort/Input/input-20000000-records-40-parts

#I would keep this setting, so that we /perf_utilities/clean_sort_output.sh can clean this file
#OUTPUTDIR=file:///perf_test/perf_harness/perf_harness/output-20000000-records-40-parts
#OUTPUTDIR=$HDFS_URL/SparkBench/Terasort/Output/ARL_terasort_output
OUTPUTDIR=hdfs://red21:48020/SparkBench/Terasort/Output/ARL_terasort_output

#mkdir /tmp/spark-events
#chmod -R 777 /tmp/spark-events

DATEFORNMON=$(./start_nmon_monitor.sh)

PERF_DATA=/perf_test/perf_data

echo "DATEFORNMON=[$DATEFORNMON]"
echo $DATEFORNMON > /tmp/timestamp.run
echo $DATEFORNMON > /perf_test/perf_data/steps-$NUM_STEPS--sleep-$STEP_OFFSET--iters-$STEP_ITERATIONS.txt
echo ""
echo "Output of 'env':"
env
echo ""
echo "Output of $SPARK_CONF_DIR/spark-defaults.conf:"
cat $SPARK_CONF_DIR/spark-defaults.conf
echo ""
echo "Output of $SPARK_CONF_DIR/spark-env.sh:"
cat $SPARK_CONF_DIR/spark-env.sh
echo ""


COUNTER=0
while [ $COUNTER -lt $NUM_STEPS ]; do
    echo "Starting Step : $COUNTER"
    ./single_stream_sequential.sh $NUMPARTITIONS $INPUTDIR $OUTPUTDIR $MASTER $STEP_ITERATIONS $COUNTER > /perf_test/perf_data/driver_output_${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $STEP_OFFSET
done

wait

#<<COMMENT_OUT_NMON
./stop_nmon_monitor.sh $DATEFORNMON
echo "nmon data stored with timestamp of $DATEFORNMON"

#echo "cleaning output dir $OUTPUTDIR ... "
#rm -rf $OUTPUTDIR*
#hadoop fs -rm -r $OUTPUTDIR*
#echo "cleaning output dir $OUTPUTDIR ... Done!"

# below collecting logs nmon and creating processed timing data
cd $PERF_DATA
mkdir $DATEFORNMON #dir should exist since we call stop_nmon_monitor.sh
chmod -R 777 $DATEFORNMON
sudo mv single-stream-results_*.txt  *.tar.gz single-stream-*-job.log driver_output_*.log env_output.log $DATEFORNMON/
sudo mv steps-*--sleep-*--iters-*.txt $DATEFORNMON/
/perf_test/perf_utilities/process-timings.sh /perf_test/perf_data/$DATEFORNMON #process timings
echo "steps=[$NUM_STEPS] sleep=[$STEP_OFFSET] iters=[$STEP_ITERATIONS]"
echo "nmon data stored and processed timings stored in /perf_test/perf_data/$DATEFORNMON"
#COMMENT_OUT_NMON

echo "Step-up complete!"
