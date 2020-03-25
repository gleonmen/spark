package com.sundogsoftware.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object Challenge {

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
    val t0 = System.nanoTime()
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val lines = spark.sparkContext.textFile("C:/sparkscala/nginx.log")
    val logs = lines.flatMap(parseLine)

   
    //SPARK 2.0  with SQL
    //Define Schema
    import org.apache.spark.sql.types._
    val schemaString = "agent request"
    val fields = schemaString.split(" ").map(fieldName => StructField(
      fieldName,
      StringType, nullable = true))
    val schema = StructType(fields)

    val t20 = System.nanoTime()
    println("started " + t20)
    val infoTable = logs.map(x => org.apache.spark.sql.Row(x._1, x._2))
    val sqlDf = spark.createDataFrame(infoTable, schema)

    sqlDf.createOrReplaceTempView("agent_request")
    val agentsSql = spark
      .sql(
        """SELECT agent, count(agent) as total_agents 
          FROM agent_request
          GROUP BY agent
          ORDER BY count(agent) DESC
          LIMIT 5""")
    agentsSql.show()

    //6.4 minutes
    val requestSql = spark
      .sql(
        """SELECT request, count(request) as total_request 
          FROM agent_request
          GROUP BY request
          ORDER BY count(request) DESC
          LIMIT 5""")
    requestSql.show()
    //shutdown spark context
    spark.stop()

    val t21 = System.nanoTime()

    //15 minutes
    println("Elapsed time: " + (t21 - t20) + "ns")

  }

}