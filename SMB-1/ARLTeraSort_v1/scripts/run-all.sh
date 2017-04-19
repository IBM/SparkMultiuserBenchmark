echo "IBM-ARL-teraSort"
if [ "$#" -ne 3 ]; then
  echo "Usage:"
  echo "./scripts/run-all \\"
  echo "[NUMBER OF INPUT RECORDS TO GENERATE]\\"
  echo "[NUMBER OF PARALLEL PROCESSES USED TO GENERATE AND VALIDATE THE DATA (but not sort)] \\"
  echo "[NUMBER OF PARTITIONS]\\"
  #echo "[INPUT DIRECTORY OF THE SORT FILES]"
  #echo "[OUTPUT DIRECTORY OF THE SORT FILES]"
  exit 1
fi

RECORDS=$1
PAR_PROC=$2
PARTITIONS=$3

SORT_INPUT_DIR="./input-$RECORDS-records-$PARTITIONS-parts"
SORT_OUTPUT_DIR="./output-$RECORDS-records-$PARTITIONS-parts"
MODE="local[*]"

echo "About to generate the data"
time ./scripts/generate.sh $RECORDS $PAR_PROC $SORT_INPUT_DIR $PARTITIONS

echo "About to sort the data"
time  ./scripts/sort.sh $PARTITIONS $SORT_INPUT_DIR $SORT_OUTPUT_DIR $MODE

echo "About to validate the data"
# Official validate (takes forever)
#time ./scripts/validate.sh $PARTITIONS $PAR_PROC $SORT_OUTPUT_DIR
# Fadi's python script
./scripts/teraval.py $SORT_OUTPUT_DIR part- -threads 24
