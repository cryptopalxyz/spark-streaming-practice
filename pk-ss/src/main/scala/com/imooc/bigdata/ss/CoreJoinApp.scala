package com.imooc.bigdata.ss

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer


object CoreJoinApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[2]").setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(sparkConf)

    //val ssc = new StreamingContext(sparkConf, Seconds(1))

    val list = new ListBuffer[(String, Boolean)]()
    list.append(("pk", true))

    val listRDD = sc.parallelize(list)

    val input = new ListBuffer[(String, String)]
    input.append(("pk","20221212,pk"))
    input.append(("test","20221212,test"))

    val inputRDD = sc.parallelize(input)

    val filterRDD = inputRDD.leftOuterJoin(listRDD)

    filterRDD.foreach(println)

    filterRDD.filter(x=>{x._2._2.getOrElse(false) !=true }).map(x=>{x._2._1}).foreach(println)


    sc.stop()
  }

}
