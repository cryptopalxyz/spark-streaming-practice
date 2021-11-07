package com.imooc.bigdata.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.reflect.ClassTag

/*
SS编程范式 with state
1)拿到ssc<=sparkConf,
2)业务逻辑
3)启动流作业

 */
object StateNetworkWordCountApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName("ddd")
    val ssc = new  StreamingContext(sparkConf, Seconds(5))

    ssc.checkpoint("ps-ss")

    //对接数据

    val lines = ssc.socketTextStream("localhost", 9527)


    //业务逻辑处理
    //方法转化为函数 _
    //ETA
    val result = lines.flatMap(_.split(',')).map((_,1)).updateStateByKey[Int](updateFunction _)
    //.reduceByKey(_+_)

    //updateStateByKey
    result.print()
    //启动
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(newValues: Seq[Int], runningCount: Option[Int]): Option[Int] = {

    // 使用新值结合已有的老的值进行fun的操作

    val current: Int = newValues.sum
    val old: Int = runningCount.getOrElse(0)

    Some(current + old)
  }


}

