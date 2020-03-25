package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.matching.Regex


object SebasLogSolution {
  //val regex: Regex = """^(?:\d+\.?)+\s(?:.+)\s(?:.+)\[(.*)\s\+.*\]\s\"((?:GET|POST)\s[^?"\s]+)[^"]*\"\s(?:\d+)\s(?:\d+)\s(?:\"[^"]+\")\s\"([^"]+)\"$""".r
  val lineRegex: String = """(?<=\-|\d|\"|\])(\s)(?=\-|\d|\"|\[)"""
  val requestRegex: String = """\"((?:GET|POST)\s[^?"\s]+)[^"]*\""""
  val agentRegex: String = """\"([^"]+)\""""
  val dateRegex: String = """\[(.*)\s\+.*\]"""
  implicit class personalizedStringFormat(text: String) {
    def dateFormat: String = text.replaceFirst(dateRegex, "$1")
    def requestFormat: String = text.replaceFirst(requestRegex, "$1")
    def agentFormat: String = text.replaceFirst(agentRegex, "$1")
  }
  def toLog(line: String) : BLog = {
    val values = line.split(lineRegex)
    values.length match {
      case 9 =>  BLog(values(3).dateFormat , values(4).requestFormat, values(8).agentFormat)
      case _ =>  BLog("invalid-date", "invalid-request", "invalid-agent")
    }
  }
  final case class BLog(date:String, request: String, agent: String)
  def main(args: Array[String]) {
    val months = args(0).toInt
    val spark = SparkSession
      .builder
      .appName("BLogs")
      .master("local[*]")
      .getOrCreate()
    val lines = spark.sparkContext.textFile("nginx")
    val logs = lines.map(toLog)
    import spark.implicits._
    val logsDS = logs.toDS()
    val filteredLogsDS = logsDS.filter(logsDS("date") =!= "invalid-date" && to_date(logsDS("date"), """dd/MMM/yyyy:HH:mm:ss""") >=  add_months(current_date(),-months)).cache()
    filteredLogsDS.groupBy("agent").count
      .orderBy(desc("count"))
      .coalesce(1).write
      .option("header", "true")
        .csv("agents")
    filteredLogsDS.groupBy("request")
      .count()
      .orderBy(desc("count"))
      .coalesce(1).write
      .option("header", "true")
      .csv("requests")
    spark.stop()
  }
}