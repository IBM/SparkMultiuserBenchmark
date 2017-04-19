#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
    echo "Cleaning terasort output directories on ${LINE}"
    ssh root@${LINE} 'rm -r ~/perf_harness/output*' &
    sleep 0.1
done
echo "Complete!"
exit 1
