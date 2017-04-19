HOSTS=`cat /perf_test/perf_harness/agent_list`
#Use when zookeeper is running e.g.
#MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)
#Use in the absence of zookeeper
MASTER=`cat /perf_test/perf_harness/master_list`


for LINE in $HOSTS
do
	nohup ssh root@$LINE "iperf -c $MASTER" &
	sleep 1
done

