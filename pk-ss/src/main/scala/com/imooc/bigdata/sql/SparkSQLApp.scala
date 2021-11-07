package com.imooc.bigdata.sql

import org.apache.spark.sql.SparkSession

object SparkSQLApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("aa")
      .master("local[2]")
      .getOrCreate()

    val df = spark.read.format("json").load("pk-ss/data/emp.json")
    df.printSchema()
    df.show()

    df.createOrReplaceTempView("emp")

    spark.sql(
      """
        |select
        |name, salary
        |from emp
        |
        |""".stripMargin).show()

    spark.stop()

  }

}
