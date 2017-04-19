package com.ibm.platform.benchmarks.smb2.analysis


import org.apache.spark._
import java.io._
import java.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

object SMB2Analysis extends App {
  
  
  override def main(args: Array[String]): Unit = {
  
    // Arguments:
    // 1 - path to the directory where the query stream files are stored
    
    // Statistics to calculate:
    // 1 - overall test duration
    // 2 - overall combined query throughput
    // 3 - overall average weighted fairness
    // 4 - Per-query statistics
    //    1 - write out time-sorted files for each query, containing only that query's data
    //    2 - max duration
    //    3 - min duration
    //    4 - avg. duration
    //    5 - std.deviation
    //    6 - 90th percentile duration
    //    7 - std. dev. steady state* - if time permits and there is 
    
    var dirPath:String = null
    
    if (args.length == 1) {
      // Initialize parameter variable
      dirPath = args(0)

      // Starting analysis
      println("Starting analysis on query stream data in directory: " + dirPath)
      
    } else {
      
      // Print out the correct usage of the application and exit
      println("Incorrect number of parameters! Please provide path to the directory containing query stream data.")
      
      return
      
    }
    
    // Create Spark context and HiveContext
    val sc = SparkContext.getOrCreate()
   // val conf = new SparkConf().setAppName("SMB2Analysis").setMaster("local").set("packages", "com.databricks:spark-csv_2.10:1.5.0").set("jars","/home/mgenkin/spark-1.6.1-bin-hadoop2.6/lib/spark-csv_2.10-1.5.0.jar,/home/mgenkin/spark-1.6.1-bin-hadoop2.6/lib/commons-csv-1.4.jar")
    //val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    val dataPath = dirPath + "/query-stream-results_*"
    val timing_data = sc.textFile(dataPath)
    
    
    val timingDataSchema =
    StructType(
     StructField("query_number", IntegerType, false) ::
     StructField("query_id", StringType, false) ::
     StructField("time_stamp", StringType, false) ::
     StructField("query_duration", IntegerType, false) :: Nil)
     
   
    val timingDataRowRDD = timing_data.map(_.split(",", -1)).map(p => Row(
          if (p(0).trim == null || (p(0).trim == "")) 0 else p(0).trim.toInt,
          p(1).trim,
          p(2).trim,
          if (p(3).trim == null || (p(3).trim == "")) 0 else p(3).trim.toInt))
    
    val timingDataFrame = sqlContext.createDataFrame(timingDataRowRDD, timingDataSchema)
    timingDataFrame.registerTempTable("timing_data")
    
    // Write out consolidated timing file
    val allTimingDF = sqlContext.sql("select * from timing_data")
    printTimingCSV(allTimingDF, dirPath + "/all_timings.csv")
    
    // Print query timing csv for Q19
    val q19DF = sqlContext.sql("select * from timing_data where query_id = 'Q19'")
    if (q19DF.count() > 0)
      printTimingCSV(q19DF, dirPath + "/Q19.csv")
    
    // Print query timing csv for Q42
    val q42DF = sqlContext.sql("select * from timing_data where query_id = 'Q42'")
    if (q42DF.count() > 0)
      printTimingCSV(q42DF, dirPath + "/Q42.csv")
   
   // Print query timing csv for Q52
    val q52DF = sqlContext.sql("select * from timing_data where query_id = 'Q52'")
    if (q52DF.count() > 0)
      printTimingCSV(q52DF, dirPath + "/Q52.csv")
      
   // Print query timing csv for Q55
    val q55DF = sqlContext.sql("select * from timing_data where query_id = 'Q55'")
    if (q55DF.count() > 0)
      printTimingCSV(q55DF, dirPath + "/Q55.csv")
      
   // Print query timing csv for Q63
    val q63DF = sqlContext.sql("select * from timing_data where query_id = 'Q63'")
    if (q63DF.count() > 0)
      printTimingCSV(q63DF, dirPath + "/Q63.csv")
      
    // Print query timing csv for Q68
    val q68DF = sqlContext.sql("select * from timing_data where query_id = 'Q68'")
    if (q68DF.count() > 0)
      printTimingCSV(q68DF, dirPath + "/Q68.csv")
      
    // Print query timing csv for Q73
    val q73DF = sqlContext.sql("select * from timing_data where query_id = 'Q73'")
    if (q73DF.count() > 0)
      printTimingCSV(q73DF, dirPath + "/Q73.csv")
      
    // Print query timing csv for Q98
    val q98DF = sqlContext.sql("select * from timing_data where query_id = 'Q98'")
    if (q98DF.count() > 0)
      printTimingCSV(q98DF, dirPath + "/Q98.csv")
     
    
    
  }
  
  def printTimingCSV(df: DataFrame, fileName: String) {
    
    // Create the timing file
    val pw = new PrintWriter(new File(fileName))  
    
    val rowArray = df.collect()
    
    var i = 0
    
    for (i <- 0 to rowArray.length-1){
      
      val currentRow = rowArray(i)
      pw.println(currentRow.getInt(0) + "," + currentRow.getString(1) + "," + currentRow.getString(2) + "," + currentRow.getInt(3))
      
    }
    
    pw.close()  
    
  }
  
  
}