#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
    echo "Copying input dir on ${LINE}"
    ssh root@${LINE} 'mkdir /perf_test/perf_harness; sudo cp -a /root/spark-1.3.1-bin-hadoop2.6/IBM_ARL_teraSort_v1-1/input-100000000-records-5-parts /perf_test/perf_harness/input-100000000-records-5-parts' 
done
echo "Complete!"
exit 1
