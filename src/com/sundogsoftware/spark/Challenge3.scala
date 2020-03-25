package com.sundogsoftware.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Challenge3 {

  /** A function that splits a line of input into (age, numFriends) tuples. */

  // ip ? ? [fecha] "request" statuscode ? ? "user-agent"
  def parseLine(line: String): Option[(String, String)] = {
    // Split by commas
    val fields = line.split("""(?<=\-|\d|\"|\])(\s)(?=\-|\d|\"|\[)""")
    if (fields.length == 9) {
      return Some((fields(4).toString, fields(8).toString))
    } else {
      return None // flatmap will just discard None results, and extract data from Some results.
    }
  }

  //lo que se necesitaba es sacar una estadistica de cuales son los requests mas comunes y cuales los user-agent mas usado

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    //SPARK 1.0
    // Create a SparkContext using every core of the local machine
    
    val t1 = System.nanoTime()
    println("Start: " + t1  + " ns")

    val conf = new SparkConf()
             .setMaster("local[*]")
             .setAppName("challengeNGnix")
             .set("spark.executor.memory", "2g")
             .set("spark.driver.memory","2g")
             .set("spark.yarn.executor.memoryOverhead","768m")
             .set("spark.executor.instances","8")
             
             
    val sc = new SparkContext(conf)
    
    
    val logFile = sc.textFile("C:/sparkscala/nginx.log")
    val logInfoLines = logFile.flatMap(parseLine).cache()

    val commonsRequest = logInfoLines.map(x => (x._1, 1)).reduceByKey((x, y) => x + y)
    val totalByrequest = commonsRequest.map(x => (x._2, x._1))
    val other = totalByrequest.sortByKey(false, 20).collect()
    println("common Request: ")
    commonsRequest.foreach(x => println(s"value ${x._2} request ${x._1}"))

    //most common user-agents
    println("common User Agents: ")
    //val commonsUserAgent = logInfoLines.map(x => (x._2, 1)).reduceByKey((x, y) => x + y)
    //val totalByUserAgent = commonsUserAgent.map(x => (x._2, x._1)).sortByKey(false).collect()
    
    //totalByUserAgent.take(5).foreach(x => println(s"value ${x._1} request ${x._2}"))
    

    
    val t2 = System.nanoTime()

    println("Elapsed time: " + (t2 - t1) + "ns")

  }

}