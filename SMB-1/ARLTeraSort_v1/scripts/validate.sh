echo "processing output"

if [ "$#" -ne 3 ]; then
  echo "Usage:"
  echo "./scripts/validate.sh [NUMBER OF OUTPUT PARTITIONS (This must match exactly with output of sort!)\]"
  echo "  [NUMBER OF PARALLEL PROCESSES TO VALIDATE WITH]"
  echo "  [OUTPUT DIR]"
  exit 1
fi

PARTITIONS=$1
PARTITIONS_DELIMITER=`expr $PARTITIONS - 1`
PAR_PROC=$2
SORT_OUTPUT_DIR=$3

echo "Removing all files in ./output-processed"
mkdir ./output-processed
rm -r ./output-processed/*

#for i in {0..$PARTITIONS_DELIMITER}
for i in `seq 0 ${PARTITIONS_DELIMITER}`
do 
  #echo $(printf %05d $i)
  cat $SORT_OUTPUT_DIR/part-$(printf %05d $i) >> output-processed/intermediate
done

#cp ./output/sort-output/part-${i} >> output-processed/intermediate

echo "Removing first bracket to match (but not make identical) to the expected output format"
echo "Saving in output-processed"
cut -c1 --complement ./output-processed/intermediate > output-processed/sort-output-processed

echo "Validating output"
todos output-processed/sort-output-processed
./teraGenVal/valsort -t${PAR_PROC} output-processed/sort-output-processed
