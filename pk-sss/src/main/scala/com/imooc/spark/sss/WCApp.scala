package com.imooc.spark.sss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode;


object WCApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("aa")
      .getOrCreate()

    val lines = spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

    import spark.implicits._

/*
    val wordcount =  lines.as[String].flatMap(_.split(","))
        .groupBy("value")
        .count()
          wordcount.writeStream
        .format("console")
      .outputMode(OutputMode.Complete())
        .start()
        .awaitTermination()

*/

    val wordcount = lines.as[String].flatMap(_.split(','))
        .createOrReplaceTempView("wc")

    spark.sql(
      """
        |select
        |value, count(1)
        |from wc
        |group by value
        |""".stripMargin)
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()




  }

}
