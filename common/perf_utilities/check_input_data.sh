#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
    
    echo "Checking input dir on ${LINE}"
    ssh root@${LINE} 'ls -al /perf_test/perf_harness/input-100000000-records-5-parts;' 
done
echo "Complete!"
exit 1
