package com.imooc.bigdata.ss

import com.imooc.bigdata.ss.NetworkWordCountApp1.ssc
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
SS编程范式
1)拿到ssc<=sparkConf,
2)业务逻辑
3)启动流作业

 */
object NetworkWordCountApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("ddd")
    val ssc = new  StreamingContext(sparkConf, Seconds(5))


    //对接数据

    val lines = ssc.socketTextStream("localhost", 9527)


    //业务逻辑处理
    val result = lines.flatMap(_.split(',')).map((_,1)).reduceByKey(_+_)

    //updateStateByKey
    result.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
