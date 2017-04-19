echo "Starting to generate input files"

if [ "$#" -ne 4 ]; then
    echo "Usage: "
    echo "./script/generate.sh \\"
    echo "[NUMBER OF INPUT RECORDS TO GENERATE] \\"
    echo "[NUMBER OF PARALLEL PROCESSES USED TO GENERATE AND VALIDATE THE DATA (but not sort)] \\"
    echo "[INPUT DIRECTORY OF THE SORT FILES]"
    echo "[NUMBER OF PARTITIONS]"
    echo "Example: scripts/generate.sh 10000000 24 /tmp 1000"
    exit 1
fi

RECORDS=$1
PAR_PROC=$2
SORT_INPUT_DIR=$3
PARTS=$4

echo "Removing the file we want to generate"
mkdir -p $SORT_INPUT_DIR
rm -r $SORT_INPUT_DIR/*

echo "Generating a file with the following parameters:"
echo "Number of records: $RECORDS"
echo "Will generate $(( RECORDS*100/1000000 )) MB of data"
echo "Number of parallel processes to use to generate the data: $PAR_PROC"
echo "Location of $PARTS parts: $SORT_INPUT_DIR"
./teraGenVal/gensort -a -t${PAR_PROC} $RECORDS $SORT_INPUT_DIR/teraSort-input
CWD=$(pwd)
cd $SORT_INPUT_DIR
echo "split -l $(( RECORDS/PARTS + 1 )) teraSort-input PART-[sequence]"
split -l $(( RECORDS/PARTS + 1 )) teraSort-input PART-
cd $CWD
rm $SORT_INPUT_DIR/teraSort-input
echo "rm $SORT_INPUT_DIR/teraSort-input"
