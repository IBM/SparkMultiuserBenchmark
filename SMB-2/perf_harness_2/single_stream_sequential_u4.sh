if [ "$#" -ne "5" ]
then
        echo "Usage:"
        echo "[NUMBER OF ITERATION SEQUENCES]"
        echo "[DELAY BETWEEN QUERIES IN A SEQUENCE]"
        echo "[DELAY BETWEEN QUERY SEQUENCES]"
        echo "[STREAM ID]"
        echo "[PATH TO THE TPC-DS DATABASE]"
        exit 1
fi

NUMSEQITER=$1
QUERYDELAY=$2
SEQDELAY=$3
STREAMID=$4
TPCDSPATH=$5

echo "Single Stream Sequential - parameters received: "
echo "NUMSEQITER: $NUMSEQITER"
echo "QUERYDELAY: $QUERYDELAY"
echo "SEQDELAY: $SEQDELAY"
echo "STREAMID: $STREAMID"
echo "TPCDSPATH: $TPCDSPATH"


MASTER_NAME=`cat /perf_test/perf_harness_2/master_list_u4_inter | grep -v "#"`

# If MESOS master, set port to 5050
if [[ $MASTER_NAME == *"mesos"* ]]; then
    MASTER_NAME="${MASTER_NAME}:5050"
fi

echo "Single Stream Sequential - Master name $MASTER_NAME"
echo "Single Stream Sequential -  NUMBER OF ITERATION SEQUENCES: $NUMSEQITER"
CURRENTTIME=$(date "+%Y%m%d-%H%M")
PERFDIR=/perf_test/perf_data_2
FILENAME=$PERFDIR/query-stream-results_$CURRENTTIME_$STREAMID.txt
 JOBLOG=$PERFDIR/query-stream-$STREAMID-job.log

#fetch timestamp
#TIMESTAMP_FILE=/tmp/timestamp.run
#TIMESTAMP=$(cat $TIMESTAMP_FILE)

COUNTER=0
exec 3>&1 4>&2

rm -rf metastore_db

if [ "$MASTER_NAME" = "yarn" ] 
then
    
    echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --queue INTERACTIVE --name UserQueryStream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH"

    $SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --queue INTERACTIVE --name UserQueryStream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH

elif [ "$MASTER_NAME" = "mesos://red21:5050" ]
then 

    echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --conf spark.mesos.role=INTERACTIVE --name UserQueryStream$Stream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH"
    $SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --conf spark.mesos.role=INTERACTIVE --name UserQueryStream$Stream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH

else

	# We are running CwS - will add Mesos later
	echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --name UserQueryStream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH"
#    export SPARK_HOME=/opt/cws21fp-reinstall/spark201inter/spark-2.0.1-hadoop-2.7/
#    export SPARK_CONF_DIR=${SPARK_HOME}/conf
#    export PATH=${SPARK_HOME}/bin:$PATH

    . /opt/profile.cws21fp1spark201_u4_inter

    $SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.syncinteractive.UserQueryStream --master $MASTER_NAME --deploy-mode client --driver-memory 3g --name UserQueryStream_$STREAMID_$CURRENTTIME UserQueryStream.jar $NUMSEQITER $QUERYDELAY $SEQDELAY $FILENAME $TPCDSPATH

fi

wait
exec 3>&- 4>&-

echo "See $JOBDIR/$FILENAME for the timing results of all sequences."
