HOSTS=`cat /perf_test/perf_harness/agent_list`

for LINE in $HOSTS
do
	ping -c 4 $LINE
	sleep 1
done

