#!/bin/bash
HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
    echo "Calling collectd restart on ${LINE}... "
    ssh root@${LINE} "service collectd restart"
    #echo "Restarting collectd on ${LINE}... Done!"
done

echo "Complete!"
exit 1
