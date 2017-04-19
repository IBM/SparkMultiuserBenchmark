#!/bin/bash
# ------------------------------------------------------------------
# [amatevos] Multi-user step-up cleanup
#          If multi-step doesnt finish, run to collect nmons, timing data, spark logs
#
    # X 1. kill master nmon
    # 2. read timestamp from /tmp/current.run
    # X 3. move master nmon to timestamped nmon
    # 4. call ./stop_nmon_monitor.sh $TIMESTAMP
    # 5. then run last steps of step_up_multi_user.sh

# ------------------------------------------------------------------

#declare variables
TIMESTAMP_FILE=/tmp/timestamp.run
TMP=/tmp

#Use when zookeeper is running
#MASTER=$(mesos-resolve `cat /etc/mesos/zk` 2>/dev/null)

#Use in the absence of zookeeper
MASTER=`cat /perf_test/perf_harness/master_list`

NMON_FILE=node${MASTER}_
PERF_DATA=/perf_test/perf_data
RUN_DIR=$PERF_DATA/$TIMESTAMP


#fetch timestamp
TIMESTAMP=$(cat $TIMESTAMP_FILE)

if [  -z "$TIMESTAMP" -o "$TIMESTAMP" == " " ];
then
     echo "$TIMESTAMP_FILE does not exist or is empty"
     exit 2
fi

#following stops nmon on all slave nodes,
#collects them to local machine in /tmp
#runs nmon chart on all then
#and puts them in tar -cvzf $PERF_DATA/tar.gz
./stop_nmon_monitor.sh $TIMESTAMP
echo "nmon data stored with timestamp of $TIMESTAMP"

echo "cleaning output dir /root/perf_harness/output-*-records-*-parts*... "
rm -rf /perf_test/perf_harness/output-*-records-*-parts*
echo "cleaning output dir /root/perf_harness/output-*-records-*-parts*... Done!"

# below collecting logs nmon and creating processed timing data

cd $PERF_DATA
mkdir $TIMESTAMP #dir should exist since we call stop_nmon_monitor.sh
mv single-stream-results_*.txt  *.tar.gz  single-stream-*-job.log $TIMESTAMP/
mv spark-1.5.1-bin-hadoop2.6-job.log* steps-*--sleep-*--iters-*.txt $TIMESTAMP/

#process timings
/perf_test/perf_utilities/process-timings.sh $PERF_DATA/$TIMESTAMP

ls -lah $TIMESTAMP/steps-*--sleep-*--iters-*.txt
echo "nmon data stored and timing results stored in $PERF_DATA/$TIMESTAMP"
