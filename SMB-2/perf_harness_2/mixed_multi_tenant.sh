if [ "$#" -ne "5" ]
then
        echo "Usage:"
        echo "[NUMBER OF BATCH STREAMS]"
	echo "[NUMBER OF BATCH ITERATIONS]"
	echo "[WORKLOAD DELAY]"
        echo "[NUMBER OF INTERACTIVE STREAMS]"
	echo "[NUMBER OF INTERACTIVE ITERATIONS]"
        exit 1
fi

# Initialize parameters

# Parameters for batch workload
NUM_BATCH_STREAMS=$1
BATCH_STREAM_OFFSET=60
BATCH_NUM_SEQ_ITER=$2
BATCH_QUERY_DELAY=30
BATCH_SEQ_DELAY=120
BATCH_TPCDS_DB_PATH="hdfs://<YOUR_HDFS_HOST>:48020/user/root/tpcds_sfactor_24"

# Workload delay
WORKLOAD_DELAY=$3

# Parameters for interactive workload
NUM_INT_STREAMS=$4
INT_STREAM_OFFSET=60
INT_NUM_SEQ_ITER=$5
INT_QUERY_DELAY=1
INT_SEQ_DELAY=5
INT_TPCDS_DB_PATH="hdfs://<YOUR_HDFS_HOST>:48020/user/root/tpcds_sfactor_12"


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

# Start the batch workload
COUNTER=0
while [ $COUNTER -lt $NUM_BATCH_STREAMS ]; do
    echo "`date` | Starting Batch Step : $COUNTER"
    ./single_stream_async-batch_u4.sh $BATCH_NUM_SEQ_ITER $BATCH_QUERY_DELAY $BATCH_SEQ_DELAY $COUNTER $BATCH_TPCDS_DB_PATH > /perf_test/perf_data_2/driver_output_batch${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $BATCH_STREAM_OFFSET
done

# Wait for the duration of the workload offset
sleep $WORKLOAD_DELAY

# Start the interactive workload
COUNTER=0
while [ $COUNTER -lt $NUM_INT_STREAMS ]; do
    echo "`date` | Starting Interactive Step : $COUNTER"
    ./single_stream_sequential_u4.sh $INT_NUM_SEQ_ITER $INT_QUERY_DELAY $INT_SEQ_DELAY $COUNTER $INT_TPCDS_DB_PATH > /perf_test/perf_data_2/driver_output_interactive${COUNTER}.log 2>&1 &
    let COUNTER=COUNTER+1
    sleep $INT_STREAM_OFFSET
done

# Check for batch workload completion - batch workload should be calibrated to take longer
echo "Checking query result...."
TOTAL_LINES=$(($NUM_BATCH_STREAMS * $BATCH_NUM_SEQ_ITER * 5))
echo "Total number of result lines should be $TOTAL_LINES"
NUM_LINES=0
while [ $NUM_LINES -lt $TOTAL_LINES ]; do
   echo "Number of lines, $NUM_LINES, is less than total number of result lines, $TOTAL_LINES, sleep 300 seconds and check again"
   sleep 300
   NUM_LINES=`cat /perf_test/perf_data_2/query-stream-results_*int.txt |wc -l`
done
echo "We got all total number of line of result, continue..."


# Collect data
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
sudo mv *_int.txt $DATEFORNMON/
/perf_test/perf_utilities/process-timings.sh /perf_test/perf_data_2/$DATEFORNMON #process timings

hadoop fs -mkdir -p /perf_test/perf_data_2/$DATEFORNMON
hadoop fs -put /perf_test/perf_data_2/$DATEFORNMON/query-stream-results_*.txt /perf_test/perf_data_2/$DATEFORNMON
$SPARK_HOME/bin/spark-submit --master $SPARK_MASTER_URL --deploy-mode client --queue BATCH --class com.ibm.platform.benchmarks.smb2.analysis.SMB2AnalysisNew /perf_test/perf_harness_2/SMB2Analysis.jar /perf_test/perf_data_2/$DATEFORNMON
hadoop fs -rmr /perf_test/perf_data_2/$DATEFORNMON

#sudo rm -f /perf_test/perf_data_2/$DATEFORNMON/*_int.txt

echo "steps=[$NUM_STEPS] sleep=[$STEP_OFFSET] iters=[$STEP_ITERATIONS]"
echo "nmon data stored and processed timings stored in /perf_test/perf_data_2/$DATEFORNMON"
#COMMENT_OUT_NMON

date
echo "Step-up complete!"
