from pyspark import SparkContext
import subprocess
import os
import sys

#sc = SparkContext("local", "gensort")
sc = SparkContext(appName="gensort")

nworkers = 2 * int(os.environ['SPARK_WORKER_INSTANCES'])
nworkers = 48 * 48
nrecords = 1000000000
nrecords_per_worker = nrecords / nworkers
worker_dir = os.environ['SPARK_WORKER_DIR']
path = os.environ['SPARK_HOME'] + '/IBM_ARL_teraSort_v1/teraGenVal/gensort'

def gensort(i):
    offset = i * nrecords_per_worker
    cmd = "%s -a -b%d %d %s/input/data-%d" % (path, offset, nrecords_per_worker, worker_dir, i)
    sys.stdout.write("calling '%s' from worker[%d]" % (cmd, i))
    subprocess.call(cmd, shell=True)
    return i

def sort(i):
    cmd = "LC_ALL=C /usr/bin/sort -k1,10 %s/input/data-%d" % (worker_dir, i)
    cmd += " --output=%s/sorted/data-%d" % (worker_dir, i)
    sys.stdout.write("calling '%s' from worker[%d]" % (cmd, i))
    subprocess.call(cmd, shell=True)
    return 1

c = sc.parallelize(xrange(0, nworkers), nworkers)
g = c.map(gensort)
s = g.map(sort)
count = s.collect()

