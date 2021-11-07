package com.imooc.spark.sss.project


import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import java.sql.Timestamp

import redis.clients.jedis.Jedis

object SSSApp {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("aa")
      .getOrCreate()
//TODO 从以前保存[ROW]的offset里面中获取Offset

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093")
      .option("subscribe", "access-topic-prod")
      //.option("startingOffsets","""{"access-topic-prod":{"0":60000}}""")
      .load()

    df.selectExpr( "CAST(value AS STRING)")
      .as[String].map(x=> {
      val splits = x.split("\t")
      val time = splits(0)
      val ip = splits(2)


      (new Timestamp(time.toLong), DateUtils.parseToDay(time), IPUtils.parseIP(ip))

    }).toDF("ts","day","province")
      .withWatermark("ts","10 minutes" )
      .groupBy("day", "province")
      .count()
      .writeStream
      //.format("console")
      .outputMode("update")
      .foreach(new ForeachWriter[Row] {
        var client: Jedis = _

        override def open(partitionId: Long, epochId: Long): Boolean = {
           client = RedisUtils.getJedisClient
           client != null
        }

        override def process(value: Row): Unit = {

          val day: String = value.getString(0)
          val province: String = value.getString(1)
          val cnt: Long = value.getLong(2)

          val offset:String = value.getAs[String]("offset")
          //TODO use client set the offset
          client.hset("day-province-cnts-"+day,province,cnt+"")

        }

        override def close(errorOrNull: Throwable): Unit = {
          if (client != null)
            client.close()

        }
      })
      .start()
      .awaitTermination()

  }

}
