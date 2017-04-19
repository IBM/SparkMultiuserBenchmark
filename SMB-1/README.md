
The Spark Multi-User Benchmark v.1
==================================
###Introduction
The Spark Multi-User Benchmark (SMB-1) is designed to measure resource manager performance for Spark workloads. 
Review the Spark-Multi-User-Benchmark-v1.0-Ext.pdf for an overview of the benchmark.

###Components
- **perf_harness** – this directory contains the key scripts that execute the SMB-1 workload, and generate output data.
- **perf_utilities** – this directory contains helper scripts that collect and process system data, and help manage the cluster.
- **IBM_ARL_teraSort_v1** – this directory contains the TeraSort implementation developed by IBM Austin Research Lab.

###Before you begin:
On each host in your cluster:
 1. Install nmon for Linux. 
 2. Install and configure resource manager(s). See each products user guide for instructions.
 3. Install and configure Apache Spark 1.5.2 (or the latest version, current implementation was tested with 1.5.2).
 4. Install and configure Apache Hadoop 2.6.3 (or the latest version, current implementation was tested with 2.6.3).
 5. Copy the ARLTeraSort_v1 directory.

###On a selected host in your cluster from which the spark driver application will run:
 1. Install nmonchart.
 2. Copy the perf_harness and perf_utilities directories to a local directory.
 3. In the same directory that contains the perf_harness and perf_utilities files, create a data directory. Update the path for the PERF_DATA parameter in the step_up_multi_user.sh script to reflect this location.

Runtime Steps
-------------
__1. Set up the Environment__
- Generate Terasort data (2 GB, 40 partitions)
```
# cd ARL_teraSort_v1/scripts
# ./generate.sh 20000000 24 /tmp/input_data_for_HDFS/input-20000000- records-40-parts 40
```
- Create an input directory and copy input data to this location. Update the path for the INPUTDIR parameter in the step_up_multi_user.sh script to reflect this location.
```
# hadoop fs -put /tmp/input_data_for_HDFS/input-20000000-records-40-parts/ /$INPUTDIR
```
- Create an Output directory in HDFS. Update the path for the OUTPUDIR parameter in the step_up_multi_user.sh script to reflect this location.

__2. Run a test__
  1. Synchronize system clocks on all nodes in the cluster.
  2. Reboot all nodes.
  3. If using mounted disks, verify that NFS servers are up and running.
  4. Startup Hadoop HDFS.
  5. Set environment variables required by the resource manager.
  6. Make desired configuration changes to spark and/or resource managers.
  7. Start resource manager and all their associated services: history server, shuffle service and web UI.
  8. Switch to the perf_harness directory and run the test:
    
  __Apache Mesos__
  ```
  # nohup ./step_up_multi_user.sh <steps> <offset> <iterations> mesos://mesos_master:5050 > /perf_test/perf_data/env_output.log 2>&1 &
  ```
  __Apache Hadoop Yarn__
  ```
  # nohup ./step_up_multi_user.sh <steps> <offset> <iterations> yarn-client > /perf_test/perf_data/env_output.log 2>&1 &
    
  Where:
    steps: is the number of users
    offset: delay in seconds before next user starts submission
    iterations: number of jobs submitted for each user
  ```
  
__3. Clean up the environment__
```
# ./clean_files_and_dirs.sh
```

__4. Verify that all jobs completed successfully__
- Check spark-events directory.
```
# grep JobSucceeded /$PATH/spark-events/* | wc -l
```
expected results = number of users * number of jobs
- Check Spark history server web UI.

__5. Verify that all required data is collected in your data directory__

The directory should contain: 

- driver*.log files containing driver app outputs
- nmon*.tar.gz file containing nmon system metrics collected during the test
- processed-stream-results.csv file containing the response-time data for the jobs
- single-stream-*.log files containing logs for each user job stream
- single-stream-results*.txt files containing detailed timing info for each user stream only
- steps*.txt file records the parameters passed into the run and the timestamp

__6. Perform analysis of data__

To analyze the data please import the  processed-stream-results.csv file into a spreadsheet of your choice, and use standard spreadsheet functions to calculate the average, standard deviation, and 90th percentile job response time. 
  
To calculate throughput you need to sort the jobs by timestap in ascending or descending order, and calculate the time difference between the latest and the earliest time-stamp. This time difference then needs to be converted to decimal-hours (e.g. 1.54 hrs). Then you would divide the total number of jobs executed during the test by this number.
