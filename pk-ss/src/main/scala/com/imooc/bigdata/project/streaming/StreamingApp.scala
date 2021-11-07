package com.imooc.bigdata.project.streaming

import com.imooc.bigdata.project.utils.{DateUtils, HBaseClient}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, TaskContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

object StreamingApp {
  def main(args: Array[String]) = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("aa")//.setMaster("local[2]")
    //sparkConf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")


    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val groupId = "pk-spark-group-1"
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9094",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("access-topic-prod")

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )


    /*
    date
    duration
    user

     every user duration for every hour
     */


    val logStream = stream.map(x=> {
      val splits = x.value().split("\t")
      (DateUtils.parseToHour(splits(0).trim), splits(1).trim.toLong, splits(5).trim)
    })//.print()

/*
    logStream.map(x=> {
      ((x._1, x._3),x._2)
    }).reduceByKey(_ + _ ).foreachRDD( rdd => {
      rdd.foreachPartition(partition => {
        val table = HBaseClient.getTable("access_user_hour")
        partition.foreach(x=> {
          table.incrementColumnValue(
            (x._1._1+"_"+x._1._2).getBytes(),
            "o".getBytes(),
            "time".getBytes(),
            x._2
          )
        })
        table.close()
      })
    })
*/
  logStream.map(x=> {
    ((x._1.substring(0,8), x._3), x._2)
  }).reduceByKey(_+_)//.print()
      .foreachRDD( rdd=> {
        rdd.foreachPartition( partition => {
          val table = HBaseClient.getTable("access_user_day")
          partition.foreach(x=> {
            table.incrementColumnValue(
              (x._1._1 + "_" + x._1._2).getBytes(),
              "o".getBytes(),
              "time".getBytes(),
              x._2)

          })
          table.close()
        })
      })


    ssc.start()
    ssc.awaitTermination()


  }
}
