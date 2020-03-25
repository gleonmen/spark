package com.sundogsoftware.spark

import org.apache.log4j._
import org.apache.spark._


object LogAnalyzer {
  case class LogStruc(ip:Ip, date:DateLog, status:StatusCode, url:UrlLog, agent: UserAgent)
  class LogComponent
  case class Ip(value: String) extends LogComponent
  case class DateLog(value: String) extends LogComponent
  case class StatusCode(verb: String, url: String, protocol: String, statusCode: Int) extends LogComponent
  case class UrlLog(value: String) extends LogComponent
  case class UserAgent(value: String) extends LogComponent
  class LogError
  case class SplitError() extends LogError
  case class ParseError() extends LogError
  trait LogParser[A] {
    def parse(in: String): A
  }
  val ipRegex: String = "\\s-\\s-\\s"
  val dateRegex: String = "\\]\\s\""
  val statusCodeRegex: String = "\\s\\d{3}\\s\""
  val urlRegex: String = "\"\\s\""
  type LogComponentTuple = (Ip, DateLog, StatusCode, UrlLog, UserAgent)
  type StringTuple = (String, String, String, String, String)
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local", "LogAnalyzer")
    val formattedData = sc.textFile("../out/nginx")
      .map(formatLog)
      .filter(_.isRight)
      .map(_.right.get)
      .map(tup => LogStruc(tup._1, tup._2, tup._3, tup._4, tup._5))
      .cache()
    println("count: " + formattedData.count())
    val agents = formattedData.map(_.agent)
      .countByValue()
      .toSeq
      .sortBy(_._2)(Ordering[Long].reverse)
      .take(5)
      .map(agent => agent._1.value + ":" + agent._2)
    println("agents: " + agents.reduce(_ + "\n" + _))
    println()
    val requests = formattedData.map(_.status)
      .map(req => req.verb + req.url)
      .countByValue()
      .toSeq
      .sortBy(_._2)(Ordering[Long].reverse)
      .take(5)
      .map(req => req._1 + ":" + req._2)
    println("requests: \n" + requests.reduce(_ + "\n" + _))
  }
  def formatLog(in: String): Either[LogError, LogComponentTuple] = {
    val tup: Either[LogError, StringTuple] = splitLog(in)
    tup.right.flatMap((tupi: StringTuple) => parseLog(tupi._1, tupi._2, tupi._3, tupi._4, tupi._5))
  }

  val ipParser: LogParser[Ip] = new LogParser[Ip] {
    override def parse(in: String): Ip = Ip(in.split("\\D").takeRight(4).reduce(_ + "." + _))
  }
  val dateParser: LogParser[DateLog] = new LogParser[DateLog] {
    override def parse(in: String): DateLog = DateLog(in.substring(1))
  }
  val statusParser: LogParser[StatusCode] = new LogParser[StatusCode] {
    override def parse(in: String): StatusCode = {
      val splitVerb = in.split(" ", 2)
      val splitUrl = splitVerb(1).split(" ", 2)
      val splitProtocol = splitUrl(1).split("\" ", 2)
      val splitStatusCode = splitProtocol(1).split(" ", 2)
      StatusCode(splitVerb(0), splitUrl(0), splitProtocol(0), splitStatusCode(0).toInt)
    }
  }
  val urlParser: LogParser[UrlLog] = new LogParser[UrlLog] {
    override def parse(in: String): UrlLog = UrlLog(in)
  }
  val agentParser: LogParser[UserAgent] = new LogParser[UserAgent] {
    override def parse(in: String): UserAgent = UserAgent(in.split(" ")(0))
  }
  def splitLog(in: String):
  Either[LogError, StringTuple] = {
    try {
      val ipSplit = in.split(ipRegex)
      val dateSplit = ipSplit(1).split(dateRegex)
      val statusSplit = dateSplit(1).split(statusCodeRegex)
      val urlSplit = statusSplit(1).split(urlRegex)
      val agentSplit = urlSplit(1)
      Right(ipSplit(0), dateSplit(0), statusSplit(0), urlSplit(0), agentSplit)
    } catch {
      case _: Throwable => Left(SplitError())
    }
  }
  def parseLog(ip: String, date: String, status: String, url: String, agent: String):
  Either[LogError, LogComponentTuple] = {
    try {
      Right(ipParser.parse(ip), dateParser.parse(date), statusParser.parse(status),
        urlParser.parse(url), agentParser.parse(agent))
    } catch {
      case _: Throwable => Left(ParseError())
    }
  }
}