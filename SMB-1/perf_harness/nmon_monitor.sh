if [ "$#" -eq "0" ]
then
        echo "Usage ./nmon_monitor.sh <time> "
        exit 2
fi

Count=$(expr $1 / 5)
HOSTS=`cat /perf_test/perf_harness/host_list`
Date=$(date +"%y%m%d_%H%M")
for LINE in $HOSTS
do
ssh -o StrictHostKeyChecking=no root@${LINE} "cd /tmp/; sudo nmon -F node${LINE}_$Date.nmon -s 5 -c $Count"
done
sleep $1
sleep 5
mkdir /tmp/nmon_$Date
mkdir /tmp/nmon_$Date/webpages
for LINE in $HOSTS
do
   scp -o StrictHostKeyChecking=no root@$LINE:/tmp/node${LINE}_$Date.nmon /tmp/nmon_$Date 
/perf_test/perf_data/nmonchart /tmp/nmon_$Date/node${LINE}_$Date.nmon /tmp/nmon_$Date/webpages/node${LINE}_$Date.html
done
tar -cvzf /tmp/nmon_$Date.tar.gz /tmp/nmon_$Date
echo "All nmon data is in dir /tmp/nmon_$Date.tar.gz"
