package com.imooc.spark.sss

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.execution.streaming.sources.ForeachDataWriter
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object SinkApp {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[2]")
      .appName("aa")
      .getOrCreate()


    mysqlSink(spark)
  }

    def fileSink(sparkSession: SparkSession) = {
      import sparkSession.implicits._

      val userSchema: StructType = new StructType()
          .add("id", IntegerType)
          .add("name", StringType)
          .add("city", StringType)

      sparkSession.readStream.format("socket")
        .option("host","localhost")
        .option("port",9999)
        .load().as[String]
        .flatMap(_.split(","))
        .map(x=> {
          (x,"pk")
        }).toDF("word","new_word")

        .writeStream
        .format("json")
        .option("path","out")
        .option("checkpointLocation","checkpoint")
        .start()
        .awaitTermination()

    }

  def kafkaSink(sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val userSchema: StructType = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("city", StringType)

    sparkSession.readStream.format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load().as[String]
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","ssstopic")
      .option("checkpointLocation","kfkacheckpoint")

      .start()
      .awaitTermination()

  }

  def mysqlSink(sparkSession: SparkSession) = {
    import sparkSession.implicits._

    sparkSession.readStream.format("socket")
      .option("host","localhost")
      .option("port",9999)
      .load().as[String]
      .flatMap(_.split(","))
      .groupBy("value")
      .count()
      .repartition(2)
      .writeStream
      .outputMode("update")
      .foreach(new ForeachWriter[Row] {
        var connection: Connection = _
        var pstmt:PreparedStatement = _
        var batchCount = 0

        override def open(partitionId: Long, epochId: Long): Boolean = {
         println(s"打开connection $partitionId, $epochId")
          Class.forName("com.mysql.jdbc.Driver")
          connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/pk","root","root")

          val sql =
            """
              |insert into t_wc(word,cnt)
              |values(?,?)
              |on duplicate key update word=?,cnt=?;
              |""".stripMargin
          pstmt = connection.prepareStatement(sql)
          connection!=null && !connection.isClosed && pstmt!=null

        }

        override def process(value: Row): Unit = {
          println(s"process ")
          val word = value.getString(0)
          val cnt = value.getLong(1).toInt

          println(s"word is $word, $cnt")

          pstmt.setString(1, word)
          pstmt.setInt(2, cnt)
          pstmt.setString(3, word)
          pstmt.setInt(4, cnt)
          pstmt.addBatch()
          batchCount += 1
          if(batchCount>10) {
            pstmt.executeBatch()
            batchCount = 0
          }

        }

        override def close(errorOrNull: Throwable): Unit = {

          println(s"关闭connection ")
          pstmt.executeBatch()
          batchCount=0
          connection.close()

        }
      })

      .start()
      .awaitTermination()

  }

}
