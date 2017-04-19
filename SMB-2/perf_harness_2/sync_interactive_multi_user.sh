if [ "$#" -ne "6" ]
then
        echo "Usage:"
        echo "[NUMBER OF STREAMS]"
        echo "[STREAM OFFSET]"
	echo "[NUMBER OF SEQUENCE ITERATIONS IN EACH STREAM]"
        echo "[DELAY BETWEEN QUERIES IN A SEQUENCE]"
	echo "[DELAY BETWEEN QUERY SEQUENCES]"
        echo "[PATH TO THE TPC-DS DATABASE]"
        exit 1
fi

NUM_STREAMS=$1
STREAM_OFFSET=$2
NUM_SEQ_ITER=$3
QUERY_DELAY=$4
SEQ_DELAY=$5
TPCDS_DB_PATH=$6

#mkdir /tmp/spark-events
#chmod -R 777 /tmp/spark-events

DATEFORNMON=$(./start_nmon_monitor.sh)

PERF_DATA=/perf_test/perf_data_2

echo "DATEFORNMON=[$DATEFORNMON]"
echo $DATEFORNMON > /tmp/timestamp.run
echo $DATEFORNMON > /perf_test/perf_data_2/steps-$NUM_STREAMS--sleep-$STREAM_OFFSET--iters-$NUM_SEQ_ITER.txt
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
date
COUNTER=0
while [ $COUNTER -lt $NUM_STREAMS ]; do
    echo "`date` | Starting Step : $COUNTER"
    ./single_stream_sequential.sh $NUM_SEQ_ITER $QUERY_DELAY $SEQ_DELAY $COUNTER $TPCDS_DB_PATH > /perf_test/perf_data_2/driver_output_${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $STREAM_OFFSET
done

wait

#<<COMMENT_OUT_NMON
./stop_nmon_monitor.sh $DATEFORNMON
# echo "nmon data stored with timestamp of $DATEFORNMON"

#echo "cleaning output dir $OUTPUTDIR ... "
#rm -rf $OUTPUTDIR*
#hadoop fs -rm -r $OUTPUTDIR*
#echo "cleaning output dir $OUTPUTDIR ... Done!"

# below collecting logs nmon and creating processed timing data
cd $PERF_DATA
mkdir $DATEFORNMON #dir should exist since we call stop_nmon_monitor.sh
chmod -R 777 $DATEFORNMON

sudo mv query-stream-results_* $DATEFORNMON/
sudo mv single-stream-results_*.txt  /perf_test/perf_data/nmon*.tar.gz single-stream-*-job.log driver_output_*.log env_output.log $DATEFORNMON/
sudo mv steps-*--sleep-*--iters-*.txt $DATEFORNMON/
/perf_test/perf_utilities/process-timings.sh /perf_test/perf_data_2/$DATEFORNMON #process timings

hadoop fs -mkdir -p /perf_test/perf_data_2/$DATEFORNMON
hadoop fs -put /perf_test/perf_data_2/$DATEFORNMON/query-stream-results_*.txt /perf_test/perf_data_2/$DATEFORNMON
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URL --deploy-mode client --class com.ibm.platform.benchmarks.smb2.analysis.SMB2AnalysisNew /perf_test/perf_harness_2/SMB2Analysis.jar /perf_test/perf_data_2/$DATEFORNMON
hadoop fs -rmr /perf_test/perf_data_2/$DATEFORNMON

echo "steps=[$NUM_STEPS] sleep=[$STEP_OFFSET] iters=[$STEP_ITERATIONS]"
echo "nmon data stored and processed timings stored in /perf_test/perf_data_2/$DATEFORNMON"
#COMMENT_OUT_NMON

date
echo "Step-up complete!"
