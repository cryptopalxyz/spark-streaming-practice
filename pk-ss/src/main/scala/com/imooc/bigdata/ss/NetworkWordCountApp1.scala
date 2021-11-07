package com.imooc.bigdata.ss

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream

/*
SS编程范式
1)拿到ssc<=sparkConf,
2)业务逻辑
3)启动流作业

 */
object NetworkWordCountApp1 extends App {



    val spark = SparkSession.builder().appName("d").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2))
/*
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)
    val ssc = new  StreamingContext(sparkConf, Seconds(5))
*/

    def readFromSocket(): Unit = {
        val socketStream: DStream[String] = ssc.socketTextStream("localhost",12345)
        val wordsStream: DStream[String] = socketStream.flatMap(line => line.split(" "))

        ssc.start()
        ssc.awaitTermination()
    }
    //对接数据

    val lines = ssc.socketTextStream("localhost", 9527)


    //业务逻辑处理
    val result = lines.flatMap(_.split(',')).map((_,1)).reduceByKey(_+_)

    result.print()
    //启动
    ssc.start()



}
