package com.imooc.bigdata.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
SS编程范式
1)拿到ssc<=sparkConf,
2)业务逻辑
3)启动流作业

 */
object FileWordCountApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("ddd")
    val ssc = new  StreamingContext(sparkConf, Seconds(1))


    //对接数据

    val lines = ssc.textFileStream("file:///Users/jguo1/ss")


    //业务逻辑处理
    val result = lines.flatMap(_.split(',')).map((_,1)).reduceByKey(_+_)

    result.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
