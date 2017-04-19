package com.ibm.platform.benchmarks.smb2.utilities

import java.io._
import java.util._

object KMeansDataGenerator extends App {
  
  
  override def main(args: Array[String]): Unit = {
    
    
    var dirPath: String = null
    var lines: Int = 0
    
    if (args.length == 2) {
      
      dirPath = args(0)
      lines = args(1).toInt
      
    } else {
      
      println("Incorrect number of parameters")
      println("Please enter the path where the kmeans_data.txt file will be generated.")
      println("Please enter the number of lines of data to generate.")
      return
    }
    
    println("Starting kmeans data generation in the following directory: " + dirPath)
    println("Starting generation of " + lines + " number of lines")
    
    // Initialize file
    val pw = new PrintWriter(new File(dirPath + "/kmeans_data.txt"))
    
    var byteCounter: Long = 0
    
    // generate row by row
    for (i <- 0 to lines) {
     
      // Random generation for now
      val r = scala.util.Random
      val c1 = math.floor(((r.nextDouble())*100))/10
      val c2 = math.floor(((r.nextDouble())*100))/10
      val c3 = math.floor(((r.nextDouble())*100))/10
      
      val line = c1.toString() + " " + c2.toString() + " " + c3.toString()
      
      byteCounter = byteCounter + line.getBytes.length
      
      println(line)
      pw.println(line)
      pw.flush()
      
    }
    
    println("Finished generating files with " + byteCounter + " bytes!")
    
    pw.close
  }
 
}