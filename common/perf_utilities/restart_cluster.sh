#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
       echo "Restarting ${LINE}"
       ssh root@${LINE} "reboot" &
    sleep 0.1
done
echo "Nodes restarting!"
reboot
exit 1
