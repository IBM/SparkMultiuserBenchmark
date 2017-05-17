# SparkMultiUserBenchmark: Phase 2
This document goes over how to setup and run the SparkMultiUserBenchmark: Phase 2 (SMB-2)
## Prerequisites 
- Many of the prerequisites for SMB-2 are similar to SMB-1. Please also refer to the README for SMB-1
- Passwordless SSH setup between master and all slaves
- MySQL Java connector installed
- Install nmon for Linux.
- Install and configure Spark resource manager(s). See each products user guide for instructions.
- Install and configure Apache Hadoop 2.7.3 (or the latest version, current implementation was tested with 2.7.3).
- Download the repository to your master host
- Create a "profile" file that contains environment information for the spark deployment under test. Note that if you have more than 1 spark deployment then you should create a profile for each one.
Below is a sample profile.

profile.platform
```
export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk/jre
export CLASSPATH=$CLASSPATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib:$SPARK_HOME/lib
export HADOOP_PREFIX=$HADOOP_HOME
export HADOOP_HOME=/opt/hadoop-2.7.3
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export HADOOP_VERSION=2_7_3
export PATH=$JAVA_HOME/bin:$JAVA_HOME/jre/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/lib
export SPARK_HOME=/opt/spark-2.0.1-bin-hadoop2.7
export SPARK_CONF_DIR=${SPARK_HOME}/yarn_conf
export PATH=${SPARK_HOME}/bin:$PATH
export CLASSPATH=$CLASSPATH:$JAVA_HOME/lib:$JAVA_HOME/jre/lib:$SPARK_HOME/lib
export SPARK_MASTER_URL=yarn
```


## Install steps
1. Unzip the downloaded repository to a location on your master host
2. Make the parent directory ``/perf_test/``
3. Create the following sub-directories:  
```
mkdir /perf_test/perf_data_2/
mkdir /perf_test/perf_utilities
mkdir /perf_test/perf_harness
mkdir /perf_test/perf_harness_2
```
*Make sure all hosts in the cluster can access the /perf_test/ directory and have read/write access*

4. Copy the repository files to your sub-directories:
```
cp SparkMultiuserBenchmark-master/common/perf_utilities/* /perf_test/perf_utilities
cp SparkMultiuserBenchmark-master/SMB-1/perf_harness/* /perf_test/perf_harness
cp SparkMultiuserBenchmark-master/SMB-2/perf_harness_2/* /perf_test/perf_harness_2
```
5. Edit /perf_test/perf_harness/master_list to contain the master host
6. Edit /perf_test/perf_harness/agent_list to contain the agent hosts
7. Edit /perf_test/perf_utilities/hosts to contain all the hosts
8. Edit ``/perf_test/perf_harness_2/master_list`` to have the URL of the spark master. Example: spark://hostA:8080
9. If you are running use case 4, you need to edit /perf_test/perf_harness_2/master_list_u4_batch, and /perf_test/perf_harness_2/master_list_u4_inter to contain the URL of the spark master which will run that particular workload.
10. Generate TPC-DS data with scale factor of your choosing. The TPC-DS kit has been included in this repository under the tpcdskit folder. Follow the readme located at https://github.com/cloudera/impala-tpcds-kit to generate your input data but use the package included in this repository. The TPC-DS kit from the Cloudera repository has outstanding defects which will prevent data generation.
11. Source the profile you created.
12. cd /perf_test/perf_harness_2/

## Running the benchmark
### Use case 1: Synchronus interactive multiuser 
```
./sync_interactive_multi_user.sh
Usage:
[NUMBER OF STREAMS]
[STREAM OFFSET]
[NUMBER OF SEQUENCE ITERATIONS IN EACH STREAM]
[DELAY BETWEEN QUERIES IN A SEQUENCE]
[DELAY BETWEEN QUERY SEQUENCES]
[PATH TO THE TPC-DS DATABASE]
```
Sample run:
```
./sync_interactive_multi_user.sh 20 60 30 5 1 "hdfs://hostA:9000/user/root/tpcds_input/" > /perf_test/perf_data_2/env_output.log 2>&1
```

### Use case 2: 
```
./async_batch_multi_user.sh
Usage:
[NUMBER OF STREAMS]
[STREAM OFFSET]
[NUMBER OF SEQUENCE ITERATIONS IN EACH STREAM]
[DELAY BETWEEN QUERIES IN A SEQUENCE]
[DELAY BETWEEN QUERY SEQUENCES]
[PATH TO THE TPC-DS DATABASE]
```

Sample run:
```
./async_batch_multi_user.sh 5 60 20 30 120 hdfs://hostA:9000/user/root/tpcds_input > /perf_test/perf_data_2/env_output.log 2>&1
```

### Use case 3:
1. Edit mixed_multi_user.sh and set BATCH_TPCDS_DB_PATH and INT_TPCDS_DB_PATH to be the input paths to the TPCDS data generated for batch and interactive workloads respectively 
```
./mixed_multi_user.sh > /perf_test/perf_data_2/env_output.log 2>&1
Usage:
[NUMBER OF BATCH STREAMS]
[NUMBER OF BATCH ITERATIONS]
[WORKLOAD DELAY]
[NUMBER OF INTERACTIVE STREAMS]
[NUMBER OF INTERACTIVE ITERATIONS]
```
Sample run:
```
./mixed_multi_user.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1
```
### Use case 4:
1. Edit single_stream_async-batch_u4.sh; change ". /opt/profile.cws21fp1spark201_u4_batch" to source the profile of the spark deployment you want to run the batch workload
2. Edit single_stream_sequential_u4.sh; change ". /opt/profile.cws21fp1spark201_u4_inter" to source the profile of the spark deployment you want to run the interactive workload
3. Edit mixed_multi_tenant.sh and set BATCH_TPCDS_DB_PATH and INT_TPCDS_DB_PATH to be the input paths to the TPCDS data generated for batch and interactive workloads respectively 
```
./mixed_multi_tenant.sh
Usage:
[NUMBER OF BATCH STREAMS]
[NUMBER OF BATCH ITERATIONS]
[WORKLOAD DELAY]
[NUMBER OF INTERACTIVE STREAMS]
[NUMBER OF INTERACTIVE ITERATIONS]
```
Sample run:
```
./mixed_multi_tenant.sh 5 15 300 5 20 > /perf_test/perf_data_2/env_output.log 2>&1
```
## Validate the results
Verify that all required data is collected in your data directory__

The directory should contain: 
- driver*.log files containing driver app outputs
- nmon*.tar.gz file containing nmon system metrics collected during the test
- query*.csv file containing the response-time data for the jobs
- steps*.txt file records the parameters passed into the run and the timestamp

## Analyze the results
To analyze the data please import the  processed-stream-results.csv file into a spreadsheet of your choice, and use standard spreadsheet functions to calculate the average, standard deviation, and 90th percentile job response time. 
  
To calculate throughput you need to sort the jobs by timestap in ascending or descending order, and calculate the time difference between the latest and the earliest time-stamp. This time difference then needs to be converted to decimal-hours (e.g. 1.54 hrs). Then you would divide the total number of jobs executed during the test by this number.
