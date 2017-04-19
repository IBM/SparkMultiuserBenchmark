if [ "$#" -eq "0" ]
then
     echo "Usage ./stop_nmon_monitor.sh <TIMESTAMP FROM start_nmon_monitor.sh>"
     exit 2
fi


Date=$1
HOSTS=`cat /perf_test/perf_harness/agent_list`
#Use when zookeeper is running
#MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)
#Use in the absence of zookeeper
MASTER=`cat /perf_test/perf_harness/master_list`

mkdir /tmp/nmon_$Date
mkdir /tmp/nmon_$Date/webpages
#pyNmonAnalyzer --defaultConfig #do this once, setup default config, then -r <configfile>


#kill nmon for master
sudo killall -9 nmon
# move nmon to directory where its brother nmon files are
# /tmp/nmon_150911_2225/node9.12.234.119_150911_2225.nmon
sudo mv /tmp/node${MASTER}_$Date.nmon /tmp/nmon_$Date/node${MASTER}_$Date.nmon
/perf_test/perf_data/nmonchart /tmp/nmon_$Date/node${MASTER}_$Date.nmon /tmp/nmon_$Date/webpages/node${MASTER}_$Date.html


for LINE in $HOSTS
do
    #ssh mesos@$LINE "pgrep nmon"
    sshpass -p LSFa4Sum ssh -o StrictHostKeyChecking=no lsfadmin@$LINE "sudo killall -9 nmon"
    sshpass -p LSFa4Sum scp -o StrictHostKeyChecking=no lsfadmin@$LINE:/tmp/node${LINE}_$Date.nmon /tmp/nmon_$Date
    /perf_test/perf_data/nmonchart /tmp/nmon_$Date/node${LINE}_$Date.nmon /tmp/nmon_$Date/webpages/node${LINE}_$Date.html
    cd /tmp/nmon_$Date/
    #pyNmonAnalyzer -i node${i}_$Date.nmon -o report_node${i} -b -x
done
for LINE in $HOSTS
do
    #ssh mesos@$i "pgrep nmon"
    #ssh mesos@$i "killall -9 nmon"
    #scp mesos@$i:/tmp/node${i}_$Date.nmon /tmp/nmon_$Date
    #/users/mesos/perf_data/nmonchart /tmp/nmon_$Date/node${i}_$Date.nmon /tmp/nmon_$Date/webpages/node${i}_$Date.html
    cd /tmp/nmon_$Date/
    #sudo pyNmonAnalyzer -i node${LINE}_$Date.nmon -o report_node${LINE} -r /perf_test/perf_harness/report.config -b -x
done
cd /perf_test/perf_harness

cd /tmp/nmon_$Date
tar -cvzf /perf_test/perf_data/nmon_$Date.tar.gz *
#echo "All nmon data is in dir /root/perf_data/nmon_$Date.tar.gz"
