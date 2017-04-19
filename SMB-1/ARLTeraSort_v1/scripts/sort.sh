echo "About to submit a sort job"

if [ "$#" -ne 4 ]; then
  echo "Usage:"
  echo "[NUMBER OF PARTITIONS]" 
  echo "[INPUT DIRECTORY OF THE SORT FILES]"
  echo "[OUTPUT DIRECTORY OF THE SORT FILES]"
  echo "[MODE/IP ADDRESS OF MASTER (example: local[*]]"
  exit 1
fi

PARTITIONS=$1 
SORT_INPUT_DIR=$2
SORT_OUTPUT_DIR=$3
MODE=$4

echo "Removing $SORT_OUTPUT_DIR"
rm -rf $SORT_OUTPUT_DIR

echo "Starting Spark.."
echo ../bin/spark-submit --class IBM_ARL_teraSort --master $MODE target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR/* $SORT_OUTPUT_DIR $PARTITIONS
../bin/spark-submit --class IBM_ARL_teraSort --master $MODE target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar $SORT_INPUT_DIR $SORT_OUTPUT_DIR $PARTITIONS

echo "Exiting Shell script.."
