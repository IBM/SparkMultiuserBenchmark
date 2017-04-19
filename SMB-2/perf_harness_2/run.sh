echo "`date`# Restart services"

egosh user logon -u Admin -x Admin; egosh service stop all
sleep 20
egosh service start "spark201-2-sparkss" "spark201-2-sparkms-batch"
egosh service start "spark201batch-sparkss" "spark201batch-sparkms-batch"
sleep 30

echo "`date`# Run with SLOTS PER EXECUTOR 9 "

./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1
sleep 30
for i in `seq 15 25`; do echo red$i; ssh red$i "echo 3 > /proc/sys/vm/drop_caches"; done
/perf_test/perf_harness/clean_files_and_dirs_ALEX.sh

sleep 120

echo "`date`# Copy new file"
cat /opt/scripts/auto/spark-env.sh
sh /opt/scripts/rcopy.sh /opt/scripts/auto/spark-env.sh /opt/cws21fp-reinstall/spark201batch/spark-2.0.1-hadoop-2.7/conf/spark-env.sh /opt/scripts/hosts

echo "`date`# Restart services"

egosh user logon -u Admin -x Admin; egosh service stop all
sleep 20
egosh service start "spark201-2-sparkss" "spark201-2-sparkms-batch"
egosh service start "spark201batch-sparkss" "spark201batch-sparkms-batch"
sleep 30

echo "`date`# Run regular run"

./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1
sleep 30
for i in `seq 15 25`; do echo red$i; ssh red$i "echo 3 > /proc/sys/vm/drop_caches"; done
/perf_test/perf_harness/clean_files_and_dirs_ALEX.sh

sleep 120

echo "`date`# Restart services"

egosh user logon -u Admin -x Admin; egosh service stop all
sleep 20
egosh service start "spark201-2-sparkss" "spark201-2-sparkms-batch"
egosh service start "spark201batch-sparkss" "spark201batch-sparkms-batch"
sleep 30

echo "`date`# Run regular run #2"
./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1
sleep 30
for i in `seq 15 25`; do echo red$i; ssh red$i "echo 3 > /proc/sys/vm/drop_caches"; done
/perf_test/perf_harness/clean_files_and_dirs_ALEX.sh


