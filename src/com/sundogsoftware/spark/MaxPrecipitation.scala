package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import scala.math.max
import java.text.SimpleDateFormat

object MaxPrecipitation {

  def parseline(line: String) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val day = fields(1).toString
    val entryType = fields(2)
    val value = fields(3).toInt
    (stationID, day, entryType, value)
  }
  
  def formatDay ( dat: String) : String = {
    val newDate = dat.substring(0,4)+"-" +dat.substring(4,6)+"-"+dat.substring(6,8)
    newDate 
  }
 

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "maxPrecipitationByDay")
    val lines = sc.textFile("C:/proyectos/02. Nilseen/SparkScala/1800.csv")

    val linesByPrecipitation = lines.map(parseline).filter(x => x._3 == "PRCP")

    //tuple ( stationID, (day, value))
    val dayTemps = linesByPrecipitation.map(x => (x._1, (x._2, x._4)))

    val results = dayTemps.reduceByKey((x,y) => if (x._2 > y._2) x else y)
    
    results.foreach( x => println(s"this station " + x._1 + " report in " + formatDay(x._2._1) + " a presipitation of  " + x._2._2))
    
     
    
 
   //val results = dayTemps.reduce((x, y) => if (x._3 > y._3) x else y)   
   // val myday= results.reduce((x,y) => if(x._2 > y._2) x else y)
   // println(s"this day ${myday._1.substring(0,4)+"-" +myday._1.substring(4,6)+"-"+myday._1.substring(6,8)}  precipitation was ${myday._2}" )
    
  
  }


}