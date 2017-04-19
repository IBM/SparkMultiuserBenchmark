#!/bin/bash
if [ "$#" -ne "1" ]
then
        echo "Usage:"
        echo "[DIRECTORY NAME]"
        exit 1
fi

DIR_NAME=$1
HOSTS=`cat /perf_test/perf_harness/host_list`

echo "about to remove $DIR_NAME"

for LINE in $HOSTS
do
    
    echo "Cleaning terasort output directories on ${LINE}"
    #ssh -o StrictHostKeyChecking=no root@${LINE} "hadoop fs -rm -r /SparkBench/Terasort/Output/*" &
    ssh -o StrictHostKeyChecking=no root@${LINE} "cd /opt/Output; rm -rf $DIR_NAME" &
    sleep 0.1
done
echo "Complete!"
exit 1
