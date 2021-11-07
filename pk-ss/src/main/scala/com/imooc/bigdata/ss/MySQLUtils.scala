package com.imooc.bigdata.ss

import java.sql.{Connection, DriverManager}

object MySQLUtils {
  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver");
    DriverManager.getConnection("jdbc:mysql://localhost:3306/pk","root","root")
  }

  def closeConnection(connection: Connection) = {
    if ( connection != null) {
      connection.close()
    }

  }

}
