package com.imooc.bigdata.ss

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer


object TransformApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)



    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 这里的编程模型是RDD
    val data = List("pk")
    val dataRDD = ssc.sparkContext.parallelize(data).map(x => (x,true))


    // TODO... 对接网络数据
    val lines = ssc.socketTextStream("localhost", 9527)


    /**
     * 20221212,pk   => (pk , 20221212,pk )
     * 20221212,test
     */
    // 这里的编程模型是DStream   DStream join RDD
    //RDD 和DStream一起操作
    lines.map(x => (x.split(",")(1), x))
      .transform(y => {
        y.leftOuterJoin(dataRDD)
          .filter(x => {
            x._2._2.getOrElse(false) != true
          })//.map(x=>x._2._1)
      }).print()


    ssc.start()
    ssc.awaitTermination()
  }

}
