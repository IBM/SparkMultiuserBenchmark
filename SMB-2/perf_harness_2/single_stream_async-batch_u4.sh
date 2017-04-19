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

# Query/job sequences
echo "Initializing sequences"
#sequence0 = (Q3 Q8 Q53 Q89 KMS)
sequence[0,0]="Q3"
sequence[0,1]="Q8"
sequence[0,2]="Q53"
sequence[0,3]="Q89"
sequence[0,4]="KMS"

#sequence1 = (KMS Q53 Q89 Q8 Q3)
sequence[1,0]="KMS"
sequence[1,1]="Q53"
sequence[1,2]="Q89"
sequence[1,3]="Q8"
sequence[1,4]="Q3"

#sequence2 = (Q53 Q3 KMS Q89 Q8)
sequence[2,0]="Q53"
sequence[2,1]="Q3"
sequence[2,2]="KMS"
sequence[2,3]="Q89"
sequence[2,4]="Q8"

#sequence3 = (Q89 KMS Q8 Q3 Q53)
sequence[3,0]="Q89"
sequence[3,1]="KMS"
sequence[3,2]="Q8"
sequence[3,3]="Q3"
sequence[3,4]="Q53"

# Select one of the sequences at random
# r=$(( $RANDOM % 4 ))

MASTER_NAME=`cat /perf_test/perf_harness_2/master_list_u4_batch | grep -v "#"`

# If MESOS master, set port to 7077
if [[ $MASTER_NAME == *"mesos"* ]]; then
    MASTER_NAME="${MASTER_NAME}:7077"
fi

echo "Single Stream Sequential - Master name $MASTER_NAME"
echo "Single Stream Sequential -  NUMBER OF ITERATION SEQUENCES: $NUMSEQITER"
CURRENTTIME=$(date "+%Y%m%d-%H%M")
PERFDIR=/perf_test/perf_data_2
#FILENAME=$PERFDIR/query-stream-results_$CURRENTTIME_$STREAMID.txt
 JOBLOG=$PERFDIR/query-stream-$STREAMID-job.log

#fetch timestamp
#TIMESTAMP_FILE=/tmp/timestamp.run
#TIMESTAMP=$(cat $TIMESTAMP_FILE)

COUNTER=0
exec 3>&1 4>&2

rm -rf metastore_db

# Iterate sequences
SEQCOUNTER=0
while [ $SEQCOUNTER -lt $NUMSEQITER ]
do 

	r=$(( $RANDOM % 4 ))
	echo "Selected sequence: $r"
	# Iterate over all queries/jobs in the sequence
	QUERYCOUNTER=0
	
	while [ $QUERYCOUNTER -lt 5 ] 
	do

                if [ "$MASTER_NAME" = "yarn" ] 
		then
			# echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --queue BATCH --name UserAsyncQueryStream_$STREAMID_$CURRENTTIME AsyncBatchQueries.jar $sequence$r[$QUERYCOUNTER] $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER &"

    			$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --queue BATCH --name UserAsyncQueryStream_$STREAMID_$CURRENTTIME_${sequence[$r,$QUERYCOUNTER]} AsyncBatchQueries.jar ${sequence[$r,$QUERYCOUNTER]} $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER & 
                    
			PID_SPARK_SUBMIT=$!
			
			echo "yarn cluster mode, need kill the spark-sbmit process, $PID_SPARK_SUBMIT, b/c  we don't want too many spark-submit process occupy memory"
 #                   	kill $PID_SPARK_SUBMIT
                
                elif [ "$MASTER_NAME" = "mesos://red21:7077" ]
                then

                        echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --conf spark.mesos.role=BATCH --name UseAsyncQueryStream_$STREAMID_$CURRENTTIME AsyncBatchQueries.jar ${sequence[$r,$QUERYCOUNTER]} $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER &"
                        $SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --conf spark.mesos.role=BATCH --name UseAsyncQueryStream_$STREAMID_$CURRENTTIME_${sequence[$r,$QUERYCOUNTER]} AsyncBatchQueries.jar ${sequence[$r,$QUERYCOUNTER]} $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER &

		else
			# We are running CwS - will add Mesos later
			# echo "$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --name UserAsyncQueryStream_$STREAMID_$CURRENTTIME AsyncBatchQueries.jar $sequence$r[$QUERYCOUNTER] $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER &"
#                        export SPARK_HOME=/opt/cws21fp-reinstall/spark201/spark-2.0.1-hadoop-2.7/
#                        export SPARK_CONF_DIR=${SPARK_HOME}/conf
#                        export PATH=${SPARK_HOME}/bin:$PATH
                        . /opt/profile.cws21fp1spark201_u4_batch
    			$SPARK_HOME/bin/spark-submit --class com.ibm.platform.benchmarks.smb2.asyncbatch.AsyncBatchQueries --master $MASTER_NAME --deploy-mode cluster --driver-memory 3g --name UserAsyncQueryStream_$STREAMID_$CURRENTTIME_${sequence[$r,$QUERYCOUNTER]} AsyncBatchQueries.jar ${sequence[$r,$QUERYCOUNTER]} $TPCDSPATH $PERFDIR/ $STREAMID $SEQCOUNTER & 

		fi

		((QUERYCOUNTER++))
		
		sleep $QUERYDELAY
                echo "Killing $PID_SPARK_SUBMIT"
                kill $PID_SPARK_SUBMIT
	done	

	((SEQCOUNTER++))
	sleep $SEQDELAY

done

wait
exec 3>&- 4>&-

echo "See $JOBDIR for the timing results of all sequences."
