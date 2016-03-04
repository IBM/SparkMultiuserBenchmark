#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/host_list`

for LINE in $HOSTS
do
    echo "Space on ${LINE}"
     ssh root@${LINE} 'df -H' 
done
echo "Complete!"
exit 1
