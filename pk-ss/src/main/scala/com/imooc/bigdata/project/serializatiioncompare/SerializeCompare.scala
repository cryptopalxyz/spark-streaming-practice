
package com.imooc.bigdata.project.serializatiioncompare
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random
import scala.collection.mutable.ArrayBuffer

case class Info(name: String ,age: Int,gender: String,addr: String)

object SerializeCompare {
  def main(args: Array[String]) {

    val conf = new SparkConf()//.setMaster("local[2]")
      .setAppName("KyroTest")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Info]))
    val sc = new SparkContext(conf)

    val arr = new ArrayBuffer[Info]()

    val nameArr = Array[String]("lsw","yyy","lss")
    val genderArr = Array[String]("male","female")
    val addressArr = Array[String]("beijing","shanghai","shengzhen","wenzhou","hangzhou")

    for(i <- 1 to 1000000){
      val name = nameArr(Random.nextInt(3))
      val age = Random.nextInt(100)
      val gender = genderArr(Random.nextInt(2))
      val address = addressArr(Random.nextInt(5))
      arr.+=(Info(name,age,gender,address))
    }

    val rdd = sc.parallelize(arr)

    //序列化的方式将rdd存到内存
    rdd.persist(StorageLevel.MEMORY_ONLY_SER)
    rdd.count()
  }
}