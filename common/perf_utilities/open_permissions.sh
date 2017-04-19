#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
    echo "Opening permissions on ${LINE}"
    ssh root@${LINE} 'chmod -R 777 /perf_test/perf_harness/input-100000000-records-5-parts' 
done
echo "Complete!"
exit 1
