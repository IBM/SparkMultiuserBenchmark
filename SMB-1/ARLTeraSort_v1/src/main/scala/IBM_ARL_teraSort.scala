/*
 * IBM_ARL_teraSort.scala - IBM internal use only
 * 
 * Simple teraSort implementation 
 *
 * Author: Tom Hubregtsen - tshubreg@ibm.austin.com
 * 
 */ 

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object IBM_ARL_teraSort {
  def main(args: Array[String]) {
    
    if (args.length != 3) {
      println("SPARK_INFO: usage:")
      println("SPARK_INFO: bin/spark-submit --class IBM_ARL_teraSort --master [mode] " +
        "target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar [input-file] " + 
        "[output-file] [number-of-partitions]")
      println("SPARK_INFO:  ")
      println("SPARK_INFO: example:")
      println("SPARK_INFO: bin/spark-submit --class IBM_ARL_teraSort --master local[4] " +
        "target/scala-2.10/ibm-arl-terasort_2.10-1.0.jar ./teraGen ./teraOut 100")
      System.exit(0)
    }

    // Set the spark configuration and context
    val conf = new SparkConf().setAppName("IBM_ARL_teraSort")
    val sc = new SparkContext(conf)

    // Process command line arguments
    val inputFile = args(0)
    val outputFile = args(1)
    val partitions = args(2).toInt

    // Report these back to the user
    println("SPARK_INFO: TeraSort started with..")
    println("SPARK_INFO: Input path: " + args(0))
    println("SPARK_INFO: Output path: " + args(1))
    println("SPARK_INFO: Number of partitions: " + args(2))

    // Load the file
    val input = sc.textFile(inputFile, partitions)
    // Map per line to a key_value pair
    val inputRDD = input.map(x => (x.substring(0,10), x.substring(12)))
    // Sort the data
    val result = inputRDD.sortByKey(true, partitions)
    // Save the result to file
    result.saveAsTextFile(outputFile)

    println("SPARK_INFO: Program exit")
    sc.stop()
  }
}
