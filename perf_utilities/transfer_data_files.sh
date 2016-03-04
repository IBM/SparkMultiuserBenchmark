if [ "$#" -ne 1 ]; then
   echo "Usage:"
   echo "[DIRECTORY OF DATA FILES TO TRANSFER]"
   exit 1
fi

DIRECTORY=$1
HOSTS=`cat /perf_test/perf_harness/agent_list`
#Use when zookeeper is running e.g.
#MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)
#Use in the absence of zookeeper
MASTER=`cat /perf_test/perf_harness/master_list`


echo "Directory: $DIRECTORY"
for LINE in $HOSTS
do
    echo "about to copy files to $LINE"
    ssh root@$LINE "mkdir /perf_test/perf_harness; scp -r root@$MASTER:$DIRECTORY $DIRECTORY" 
    echo "finished copying files to $LINE"
    sleep 1
done
