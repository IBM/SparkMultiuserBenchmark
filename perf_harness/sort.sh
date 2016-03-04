echo "About to submit a sort job"

if [ "$#" -lt 5 ]; then
  echo "Usage:"
  echo "[NUMBER OF PARTITIONS]"
  echo "[INPUT DIRECTORY OF THE SORT FILES]"
  echo "[OUTPUT DIRECTORY OF THE SORT FILES]"
  echo "[MODE/IP ADDRESS OF MASTER (example: local[*]]"
  echo "[JOBLOG]"
  echo "[APP LABEL (optional)]"
  exit 1
fi

PARTITIONS=$1
SORT_INPUT_DIR=$2
SORT_OUTPUT_DIR=$3
MODE=$4
JOBLOG=$5
APP_LABEL=$6

echo "Removing $SORT_OUTPUT_DIR"
/opt/hadoop/bin/hadoop fs -rm -r -f $SORT_OUTPUT_DIR

echo "Starting Spark.."

#PICK ONE SECTION BELOW TO COMMENT IN
#terasort

#This is for running with Conductor on Spark 1.5.2
#echo /spark_top/sparkPerfTest/spark-1.5.2-hadoop-2.6/bin/spark-submit --master $MODE  --class IBM_ARL_teraSort target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 
#/spark_top/sparkPerfTest/spark-1.5.2-hadoop-2.6/bin/spark-submit --master $MODE --deploy-mode client --class IBM_ARL_teraSort /spark_top/sparkPerfTest/spark-1.5.2-hadoop-2.6/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG

/opt/sparkOnConductor/spark-1.5.2-hadoop-2.6/bin/spark-submit --master $MODE --deploy-mode client --class IBM_ARL_teraSort /opt/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG 

#This is for Spark on YARN on Spark 1.5.2
#echo /opt/spark_yarn/bin/spark-submit --master $MODE --class IBM_ARL_teraSort /spark_top/sparkPerfTest/spark-1.5.2-hadoop-2.6/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG
#/opt/spark_yarn/bin/spark-submit --master $MODE --class IBM_ARL_teraSort /spark_top/sparkPerfTest/spark-1.5.2-hadoop-2.6/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG

#Spark on Mesos with Spark 1.4.1
#echo /opt/spark_mesos/bin/spark-submit --master $MODE --class IBM_ARL_teraSort /spark_top/sparkPerfTest/spark-1.4.1-hadoop-2.6/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG
#/opt/spark_mesos/bin/spark-submit --master $MODE --class IBM_ARL_teraSort /opt/spark_mesos/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS  >> $JOBLOG 2>&1

#python letter counter
#echo /root/spark-1.3.1-bin-hadoop2.6/bin/spark-submit --master $MODE /root/perf_harness/test.py "$MODE" "$APP_LABEL"
#/root/spark-1.3.1-bin-hadoop2.6/bin/spark-submit --master $MODE /root/perf_harness/test.py "$MODE" "$APP_LABEL"

#Spark Pi example
#echo /opt/spark-1.5.1-bin-hadoop2.6/bin/spark-submit --master $MODE --class org.apache.spark.examples.SparkPi /opt/spark-1.5.1-bin-hadoop2.6/lib/spark-examples-1.5.1-hadoop2.6.0.jar
#/opt/spark-1.5.1-bin-hadoop2.6/bin/spark-submit --master $MODE --class org.apache.spark.examples.SparkPi /root/spark-1.5.1-bin-hadoop2.6/lib/spark-examples-1.5.1-hadoop2.6.0.jar

#END OPTIONS

echo "Exiting Shell script.."
