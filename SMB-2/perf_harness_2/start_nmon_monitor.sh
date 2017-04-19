Date=$(date +"%y%m%d_%H%M")

#----------Variables
HOSTS=`cat /perf_test/perf_harness/agent_list`
#Use when zookeeper is running
#MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)
#Use in the absence of zookeeper (IRIS environment)
MASTER=`cat /perf_test/perf_harness/master_list`


#fix for master machine,

sudo nmon -F /tmp/node${MASTER}_$Date.nmon -s 5 -c 999999


for LINE in $HOSTS

do
#sshpass -p mesosiscool ssh root@$LINE "cd /tmp/; nmon -F node${LINE}_$Date.nmon -s 1"
sshpass -p LSFa4Sum ssh -o StrictHostKeyChecking=no root@$LINE "cd /tmp/; sudo nmon -F node${LINE}_$Date.nmon -s 5 -c 999999"
done

echo $Date
