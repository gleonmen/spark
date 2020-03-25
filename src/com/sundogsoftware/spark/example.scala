package com.sundogsoftware.spark


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object example {
  
  def parseline(line: String) ={
    val fields = line.split(",")
    val firstName = fields(1).toString()
    val age = fields(3).toInt
    (firstName, age)    
  }
  
  
    def main(args: Array[String]) {
        Logger.getLogger("org").setLevel(Level.ERROR)
        val sc =  new SparkContext("local[*]", "FriendsByAge")
        
        val lines = sc.textFile("C:/proyectos/02. Nilseen/SparkScala/fakefriends.csv")
        val rdd = lines.map(parseline)
        val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1+ y._1, x._2 + y._2))
        val ageAverage = totalsByAge.mapValues( x=> x._1/x._2)
        
        val result = ageAverage.collect()
        
   
         result.sorted.foreach(println)
        
        
        
        
  }
}