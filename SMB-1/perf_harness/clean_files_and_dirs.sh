#!/bin/bash

# Cleanup Spark Data (Shuffle data, blockmgr)
echo "Cleaning Spark shuffle data, blockmgr data on each host..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i "rm -rf /mnt/?/spark/data /mnt/?/yarn/*/*"; done
sleep 1
# Recreate the /mnt/?/spark/data directories on each disk
echo "Re-creating the /mnt/<disk>/spark/data directories on each disk for each host..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i "mkdir /mnt/c/spark/data /mnt/d/spark/data /mnt/e/spark/data /mnt/f/spark/data /mnt/g/spark/data /mnt/h/spark/data /mnt/i/spark/data /mnt/j/spark/data /mnt/k/spark/data /mnt/l/spark/data /mnt/m/spark/data; chmod 777 /mnt/?/spark/data"; done
sleep 1
# Clean up leftover nmons files, Spark workdir 
echo "Cleaning leftover nmon files, Spark work dir on each host..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i "rm -rf /tmp/nmon* /tmp/node* /tmp/timestamp*" /opt/sparkConductor/spark-1.5.2-hadoop-2.6/work/app-* /tmp/mesos/* ; done
sleep 1
# Cleanup spark events (read by the history server)
echo "Cleaning Spark events directory..."
rm -rf /mnt/nfs/conductor/spark-events/* /mnt/nfs/yarn/spark-events/* /mnt/nfs/mesos/spark-events/*
sleep 1
# Cleanup HDFS
echo "Cleaning HDFS Terasort output directory..." 
source /opt/profile.conductor-spark-bench
hadoop fs -rm -r -skipTrash /SparkBench/Terasort/Output/*
# Cleanup Spark executor logs 
echo "Cleaning Spark executor logs for Conductor..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i rm -rf /var/tmp/elk_logs/251d15ed-773a-4cdc-8848-8209f6e61daf.lsfadmin.SparkBenchmark.lsfadmin/* ; done
echo "Cleaning Spark executor logs for YARN..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i rm -rf /opt/hadoop/logs/userlogs/* ; done
echo "Cleaning Spark executor logs for Mesos..."
for i in `seq 15 25`; do echo your_host$i; ssh your_host$i rm -rf /tmp/mesos/*; done
