package com.imooc.spark.sss

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructType};
import org.apache.spark.sql.functions.window


object SourceApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("aa")
      .getOrCreate()

    eventTimeWindow(spark)
  }

    def readCsv(sparkSession: SparkSession) = {
      import sparkSession.implicits._
      val userSchema: StructType = new StructType()
          .add("id", IntegerType)
          .add("name", StringType)
          .add("city", StringType)

      sparkSession.readStream.format("csv")
        .schema(userSchema)
        .load("pk-sss/data/csv")
        .groupBy("city")
        .count()
        .writeStream
        .format("console")
        .outputMode("complete")
        .start()
        .awaitTermination()

    }

  def readCsvPartition(sparkSession: SparkSession) = {
    import sparkSession.implicits._
    val userSchema: StructType = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("city", StringType)

    sparkSession.readStream.format("csv")
      .schema(userSchema)
      .load("pk-sss/data/partition")
      //.groupBy("city")
      //.count()
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def kafkaSource(spark: SparkSession): Unit = {
    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribe", "ssstopic")
      .load()

    df.selectExpr( "CAST(value AS STRING)")
      .as[String].flatMap(_.split(","))
      .groupBy("value")
      .count()
      .writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()

  }

    def eventTimeWindow(spark: SparkSession) = {

      import spark.implicits._

      /*

      2020-10-01 12:00:00,cat
      2020-10-01 12:02:00,dog
      2020-10-01 12:03:00,dog
      2020-10-01 12:03:00,dog
      2020-10-01 12:07:00,owl
      2020-10-01 12:07:00,cat
      2020-10-01 12:11:00,dog
      2020-10-01 12:13:00,owl
       */


      spark.readStream.format("socket")
        .option("host", "localhost")
        .option("port",9999)
        .load.as[String]
        .map(x=> {
          val splits = x.split(",")
          (splits(0), splits(1))
        }).toDF("ts", "word")
        .groupBy(
          window( $"ts", "10 minutes", "5 minutes"),
          $"word"
        ).count()
        .sort("window")
        .writeStream
        .format("console")
        .option("truncate", false)
        .outputMode("complete")
        .start()
        .awaitTermination()
    }
}
