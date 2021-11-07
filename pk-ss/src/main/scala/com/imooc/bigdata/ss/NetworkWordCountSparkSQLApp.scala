package com.imooc.bigdata.ss

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
SS编程范式
1)拿到ssc<=sparkConf,
2)业务逻辑
3)启动流作业

 */
object NetworkWordCountSparkSQLApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("ddd")
    val ssc = new  StreamingContext(sparkConf, Seconds(5))


    //对接数据

    val lines = ssc.socketTextStream("localhost", 9527)


    val word = lines.flatMap(_.split(','))
    word.foreachRDD(rdd=> {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val wordsDataFrame = rdd.toDF("word")
      wordsDataFrame.createOrReplaceTempView("wc")
      spark.sql(
        """
          |select word, count(*) from wc group by word
          |""".stripMargin).show()

    })

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
