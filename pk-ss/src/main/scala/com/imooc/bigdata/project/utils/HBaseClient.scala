package com.imooc.bigdata.project.utils

import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

object HBaseClient {


  private val configuration = HBaseConfiguration.create()
  configuration.set("hbase.zookeeper.quorum","localhost:2181")

  //configuration.set("hbase.zookeeper.quorum", "localhost" );

  //configuration.set("hbase.zookeeper.property.clientPort", "2181");

  //configuration.set("zookeeper.znode.parent", "/hbase-unsecure");

  private val connection = ConnectionFactory.createConnection(configuration)

  def main(args: Array[String]): Unit = {
    query()

  }

  def getTable(tableName: String) = {
    connection.getTable(TableName.valueOf(tableName))
    //println("--" + table)
    //println("=="+ table.getName)
    //table.getName

  }

  def insert(tableName: String): Unit = {
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    println("--" + table)
    println("=="+ table.getName)
    table.getName

    val put = new Put("4".getBytes())
    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan") )
    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("age"), Bytes.toBytes("33") )
    put.addColumn(Bytes.toBytes("o"), Bytes.toBytes("sex"), Bytes.toBytes("male") )

    table.put(put)
    if (table != null)
      table.close()

  }

  def query()= {
    val table: Table = connection.getTable(TableName.valueOf("user"))

    val scan = new Scan()
    scan.setStartRow("1".getBytes())
    scan.setStopRow("4".getBytes())
    scan.addColumn("o".getBytes(), "name".getBytes())
    val iterator = table.getScanner(scan).iterator()
    while (iterator.hasNext) {
      val result: Result = iterator.next()

      while (result.advance()) {
        val cell: Cell = result.current()
        val row = new String(CellUtil.cloneRow(cell))
        val cf = new String(CellUtil.cloneFamily(cell))
        val qualifier = new String(CellUtil.cloneQualifier(cell))
        val value = new String(CellUtil.cloneValue(cell))

        println(row + "..." + cf + "..." + qualifier + "... " + value)

      }
    }

  }

}
