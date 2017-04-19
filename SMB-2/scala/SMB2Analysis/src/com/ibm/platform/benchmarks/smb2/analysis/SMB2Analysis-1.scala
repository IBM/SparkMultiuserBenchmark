package com.ibm.platform.benchmarks.smb2.analysis


import org.apache.spark._
import java.io._
import java.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.hive.HiveContext

object SMB2AnalysisNew extends App {
  
  
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
    // val sc = SparkContext.getOrCreate()
    val conf = new SparkConf().setAppName("SMB2Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    val dataPath = dirPath + "/query-stream-results_*"
    val timing_data = sc.textFile(dataPath)
    
    // File schema
    // pw.println(queryID + "," + queryLabels(queryID) + "," + startDate + "," + timeStamp + "," + queryDuration + "," + rowCount + "," + firstColValue)
    
    val timingDataSchema =
    StructType(
     StructField("query_number", IntegerType, false) ::
     StructField("query_id", StringType, false) ::
     StructField("start_date", StringType, false) ::
     StructField("time_stamp", StringType, false) ::
     StructField("query_duration", IntegerType, false) ::
     StructField("row_count", IntegerType, false) ::
     StructField("row_1_data", StringType, false) :: Nil)
     
   
    val timingDataRowRDD = timing_data.map(_.split(",", -1)).map(p => Row(
          if (p(0).trim == null || (p(0).trim == "")) 0 else p(0).trim.toInt,
          p(1).trim,
          p(2).trim,
          p(3).trim,
          if (p(4).trim == null || (p(4).trim == "")) 0 else p(4).trim.toInt,
          if (p(5).trim == null || (p(5).trim == "")) 0 else p(5).trim.toInt,
          p(6).trim))
    
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
      
    // Print query timing csv for Q3
    val q3DF = sqlContext.sql("select * from timing_data where query_id = 'Q3'")
    if (q3DF.count() > 0)
      printTimingCSV(q3DF, dirPath + "/Q3.csv")
   
    // Print query timing csv for Q8
    val q8DF = sqlContext.sql("select * from timing_data where query_id = 'Q8'")
    if (q8DF.count() > 0)
      printTimingCSV(q8DF, dirPath + "/Q8.csv")
      
    // Print query timing csv for Q53
    val q53DF = sqlContext.sql("select * from timing_data where query_id = 'Q53'")
    if (q53DF.count() > 0)
      printTimingCSV(q53DF, dirPath + "/Q53.csv")
      
    // Print query timing csv for Q89
    val q89DF = sqlContext.sql("select * from timing_data where query_id = 'Q89'")
    if (q89DF.count() > 0)
      printTimingCSV(q89DF, dirPath + "/Q89.csv")
      
    // Print query timing csv for Q89
    val kmsDF = sqlContext.sql("select * from timing_data where query_id = 'KMS'")
    if (kmsDF.count() > 0)
      printTimingCSV(kmsDF, dirPath + "/KMS.csv")
      
    // Print out the query statistics summary
    // printQuerySummary(sqlContext, dirPath + "query_summary.csv")

  }
  
  def printTimingCSV(df: DataFrame, fileName: String) {
    
    // Create the timing file
    val pw = new PrintWriter(new File(fileName))  
    
    val rowArray = df.collect()
    
    var i = 0
    
    for (i <- 0 to rowArray.length-1){
      
      val currentRow = rowArray(i)
      pw.println(currentRow.getInt(0) + "," + currentRow.getString(1) + "," + currentRow.getString(2) + "," + currentRow.getString(3) + "," + currentRow.getInt(4) + "," + currentRow.getInt(5) + "," + currentRow.getString(6))
      
    }
    
    pw.close()  
    
  }
  
  def printQuerySummary(sqlContext: HiveContext, fileName: String) {
    
    val queryTemplate = "select count('query_duration'), max('query_duration'), min('query_duration'), avg('query_duration'), stddev('query_duration') from timing_data where query_id = "
    
    // Create the timing file
    val pw = new PrintWriter(new File(fileName))  
    
    // Print headings
    
    pw.println("Query" + "," + "Number Executed" + "," + "Maximum Duration (sec)" + "," + "Miminum Duration (sec)" + "," + "Average Duration (sec)" + "," + "Standard Deviation (sec)" + "," + "Fairness")
    
    // Labels
    // Store query labels for better log readability and easier data processing
    val queryLabels = Array.ofDim[String](8)
    
    queryLabels(0) = "'Q19'"
    queryLabels(1) = "'Q42'"
    queryLabels(2) = "'Q52'"
    queryLabels(3) = "'Q55'"
    queryLabels(4) = "'Q63'"
    queryLabels(5) = "'Q68'"
    queryLabels(6) = "'Q73'"
    queryLabels(7) = "'Q98'"
    
    val fairnessArray = Array.ofDim[Double](8)
    
    var i = 0
    
    for (i <- 0 to queryLabels.length-1){
      
      val querydata = sqlContext.sql(queryTemplate + queryLabels(i))
      
      if (querydata.count == 1) {
        
        val querydataarray = querydata.collect()
        val count = querydataarray(0).getLong(0)
        val max = querydataarray(0).getDouble(1)
        val min = querydataarray(0).getDouble(2)
        val avg = querydataarray(0).getDouble(3)
        val stddev = querydataarray(0).getDouble(4)
        
        fairnessArray(i) = stddev/avg*100
        
        pw.println(queryLabels(i) + "," + count + "," + max + "," + min + "," + avg + "," + stddev + "," + fairnessArray(i))
        
      }
        
    }
    
    // Calculate average fairness here
    
    
    pw.close()  
    
  }
  
  
}

