package com.sundogsoftware.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.desc

object Example1 {

  def main(args: Array[String]) {
    
    
    val spark = SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    
     val myRange = spark.range(1000).toDF("number")
    val divisBy2 = myRange.where("number % 2 = 0")
    println(divisBy2.count())

    // DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count
    //United States,Romania,15
    //United States,Croatia,1
    //United States,Ireland,344

    val flightData2015 = spark
      .read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("C:/data/flight-data/csv/2015-summary.csv")

    println(flightData2015.take(3))
    flightData2015.sort("count").explain()
    

    spark.conf.set("spark.sql.shuffle.partitions", "5")

    println(flightData2015.sort("count").take(2))

    flightData2015.createOrReplaceTempView("flight_data_2015")

    // in Scala
    val sqlWay = spark
      .sql(
        """SELECT DEST_COUNTRY_NAME, count(1) 
        FROM flight_data_2015 
        GROUP BY DEST_COUNTRY_NAME""")

    val dataFrameWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()

    println("***Same plan")

    sqlWay.explain

    dataFrameWay.explain

    spark.sql("SELECT max(count) from flight_data_2015").take(1)

    println(flightData2015.select(max("count")).take(1))

    val maxSql = spark
      .sql(
        """SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
          FROM flight_data_2015
          GROUP BY DEST_COUNTRY_NAME
          ORDER BY sum(count) DESC
          LIMIT 5""")

    maxSql.show()

    println("***Same DATA")

    flightData2015
      .groupBy("DEST_COUNTRY_NAME")
      .sum("count")
      .withColumnRenamed("sum(count)", "destination_total")
      .sort(desc("destination_total"))
      .limit(5)
      .show()

  }
}