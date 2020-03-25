package com.sundogsoftware.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Challenge2 {

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

    //SPARK 2.0

    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val t0 = System.nanoTime()

    println("Started time: " + (t0) + "ns")

    val lines = spark.sparkContext.textFile("C:/sparkscala/nginx.log")
    val logs = lines.flatMap(parseLine)

    import spark.implicits._
    val logsDS = logs.toDF("agent", "request")

    //Agents
    logsDS.groupBy("agent").count
      .orderBy(desc("count"))
      .coalesce(1).write
      .option("header", "true")
      .csv("agents")

    //REquest
    logsDS.groupBy("request")
      .count()
      .orderBy(desc("count"))
      .coalesce(1).write
      .option("header", "true")
      .csv("requests")

    val t1 = System.nanoTime()
    //12 minutes
    println("Elapsed time: " + (t1 - t0) + "ns")
  }

}