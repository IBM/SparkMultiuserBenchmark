
The Spark Multi-User Benchmark v.1
Introduction
The Spark Multi-User Benchmark (SMB-1) is designed to measure resource manager performance for Spark workloads. 
Review the Spark-Multi-User-Benchmark-v1.0-Ext.pdf for an overview of the benchmark.

Components
perf_harness directory – this directory contains the key scripts that execute the SMB-1 workload, and generate output data.
perf_utilities directory – this directory contains helper scripts that collect and process system data, and help manage the cluster.
IBM_ARL_teraSort_v1 – this directory contains the TeraSort implementation developed by IBM Austin Research Lab.


Before you begin:
On each hosts in your cluster:
1.Install nmon for Linux. 
2.Install and configure resource manager(s). See each products user guide for instructions.
3.Install and configure Apache Spark 1.5.2 (or the latest version, current implementation was tested with 1.5.2).
4.Install and configure Apache Hadoop 2.6.3 (or the latest version, current implementation was tested with 2.6.3).
5.Copy the ARLTeraSort_v1 directory.

On a selected host in your cluster:
Install nmonchart.
Copy the perf_harness and perf_utilities directories to a local directory.
In the same directory that contains the perf_harness and perf_utilities files, create a data directory. Update the path for the PERF_DATA parameter in the step_up_multi_user.sh script to reflect this location.
This host is the node from which the spark driver application will run.

Steps

1. Set up the Environment 
i.Generate Terasort data (2 GB, 40 partitions).
# cd ARL_teraSort_v1/scripts
# ./generate.sh 20000000 24 /tmp/input_data_for_HDFS/input-20000000- records-40-parts 40
Create an input directory and copy input data to this location. Update the path for the INPUTDIR parameter in the step_up_multi_user.sh script to reflect this location.
# hadoop fs -put /tmp/input_data_for_HDFS/input-20000000-records-40-parts/ /$INPUTDIR
Create an Output directory in HDFS. Update the path for the OUTPUDIR parameter in the step_up_multi_user.sh script to reflect this location.

2.Run a test
i.Synchronize system clocks on all nodes in the cluster.
ii.Reboot all nodes.
iii.If using mounted disks, verify that NFS servers are up and running.
iv.Startup Hadoop HDFS.
v.Set environment variables required by the resource manager.
vi.Make desired configuration changes to spark and/or resource managers.
vii.Start resource manager and all their associated services - history server, shuffle service and web UI.
viii.  Switch to the perf_harness directory and run start the test:
	Apache Mesos
# nohup ./step_up_multi_user.sh <steps> <offset> <iterations> mesos://mesos_master:5050 > /perf_test/perf_data/env_output.log 2>&1 &   
	Apache Hadoop Yarn
# nohup ./step_up_multi_user.sh <steps> <offset> <iterations> yarn-client > /perf_test/perf_data/env_output.log 2>&1 &

           Where:
steps: is the number of users
offset: delay in seconds before next user starts submission
iterations: number of jobs submitted for each user

3. Clean up the environment.
   # ./clean_files_and_dirs.sh

4.Verify that all jobs completed successfully.
Check spark-events directory.
        # grep JobSucceeded /$PATH/spark-events/* | wc -l
	         expected results = number of users * number of jobs
Check Spark history server web UI.

5.Verify that all required data is collected in your data directory. The directory should contain: 
	1 – driver*.log files containing driver app outputs; 
	2 – nmon*.tar.gz file containing nmon system metrics collected during the test; 
	3 – processed-stream-results.csv file containing the response-time data for the jobs; 
	4 – single-stream-*.log files containing logs for each user job stream; 
	5 – single-stream-results*.txt files containing detailed timing info for each user stream only;  
	6 - steps*.txt file records the parameters passed into the run and the timestamp.

6. Perform analysis of data. 
	To analyze the data please import the  processed-stream-results.csv file into a spreadsheet of your choice, 
	and use standard spreadsheet functions to calculate the average, standard deviation, and 90th percentile job response time. 
	To calculate throughput you need to sort the jobs by timestap in ascending or descending order, and calculate the 
	time difference between the latest and the earliest time-stamp. This time difference then needs to be converted to 
	decimal-hours (e.g. 1.54 hrs). Then you would divide the total number of jobs executed during the test by this number.