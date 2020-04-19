package cn.wi.flink.mysql


import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
class SinkMySQL extends RichSinkFunction[Demon]{
  var connection:Connection=_
  var preparedStatement:PreparedStatement=_

  override def open(parameters: Configuration): Unit = {
    val driverManager = "com.mysql.jdbc.Driver"
    Class.forName(driverManager)
    val url = "jdbc:mysql://node03:3306/test"
    //获取连接
    connection = DriverManager.getConnection(url, "root", "123456")
    preparedStatement = connection.prepareStatement("insert into demon values(?,?,?)")
  }
    //数据插入操作  执行业务的主方法
    override def invoke(value: Demon): Unit = {
      preparedStatement.setInt(1, value.id)
      preparedStatement.setString(2, value.name)
      preparedStatement.setInt(3, value.age)
      preparedStatement.executeUpdate()
    }
    //关流
    override def close(): Unit = {
      if (preparedStatement != null) {
        preparedStatement.close()
      }
      if (connection != null) {
        connection.close()
      }

    }

}
