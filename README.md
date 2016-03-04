The Spark Multi-User Benchmark v.1

Introduction

The Spark Multi-User Benchmark (SMB-1) is specifically designed to measure resource manager performance for Spark workloads.
Please read the included pdf file (Spark-Multi-User-Benchmark-v1.0-Ext.pdf) for a good overview of the theory associated with
this benchmark.

Benchmark Code includes the following components:

- perf_harness directory includes the key scripts that execute the SMB-1 workload, and generate output data
- perf_utilities directory includes some helper scripts that collect and process system data, and help manage the cluster
- IBM_ARL_teraSort_v1 directory contains the TeraSort implementation developed by IBM Austin Research Lab

Running the SMB-1 Benchmark

Please note the steps below are not the same for Mesos, the step up script for Mesos is slighlty different to accommodate the different spark version as well the deploy mode.

1. Set up and install your cluster (instructions not included here, please follow Spark docs, and docs for your resource manager).

2. Copy benchmark code to the server from which Spark driver applications will be running (often resource manager master).

3. Use the gensort executable in the IBM_ARL_teraSort_v1 directory to generate data.

4. Fill in host names in the master_list, host_list, and agent_list files.

5. Run the main benchmark script as in this example: ./step_up_multi_user.sh 20 60 60 spark://spark-batch-master:7077

Please see the step_up_multi_user.sh script for explanation of parameters and how to use them. Please see comments in utility scripts
and host name files for explanation of how to use them.


