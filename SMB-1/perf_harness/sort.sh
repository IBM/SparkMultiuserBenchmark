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
echo $SPARK_HOME/bin/spark-submit --master $MODE --deploy-mode client --class IBM_ARL_teraSort /opt/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS
$SPARK_HOME/bin/spark-submit --master $MODE --deploy-mode client --class IBM_ARL_teraSort /opt/IBM_ARL_teraSort_v1-1/target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS 2>&1 >> $JOBLOG

echo "Exiting Shell script.."
