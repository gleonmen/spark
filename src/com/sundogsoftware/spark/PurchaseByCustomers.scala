package com.sundogsoftware.spark


import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object PurchaseByCustomers {
   
  def parseLine (line:String)  ={
    val fields =  line.split(",")
    val id = fields(0).toInt
    val value =  fields(2).toFloat
    (id, value)
  }
  
  def main (args:Array[String]){
   Logger.getLogger("org").setLevel(Level.ERROR)
   
   val sc = new SparkContext("local[*]", "myPurchaseByCustomer");  
   
   val rdd = sc.textFile("C:/proyectos/02. Nilseen/SparkScala/customer-orders.csv")
   
   val customerValue = rdd.map(x => parseLine(x))
   
   val cvAggregate = customerValue.reduceByKey((x,y) => x+y)
   
   val mapByValue = cvAggregate.map(x => (x._2, x._1))          
   val results =  mapByValue.collect()
    
  
   
   
    for (result <- results.sorted) {
       val id = result._2
       val temp = result._1
       val formattedTemp = f"$temp%.2f F"
       println(s"Customer $id with value : $formattedTemp") 
    }
  
  
  }
  
}