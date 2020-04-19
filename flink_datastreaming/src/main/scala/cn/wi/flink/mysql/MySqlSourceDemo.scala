package cn.wi.flink.mysql

import java.sql.PreparedStatement


import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
class MySqlSourceDemo extends RichSourceFunction[Demon] {
  var conn: Connection = _
  var pst: PreparedStatement = _
  //初始化数据源
  override def open(parameters: Configuration): Unit = {

    val driver = "com.mysql.jdbc.Driver"
    Class.forName(driver)
    val url = "jdbc:mysql://node02:3306/itcast"
    //获取连接
    conn = DriverManager.getConnection(url,"root","123456")
    pst = conn.prepareStatement("select * from demo")
    pst.setMaxRows(100) //查询最大行数
  }

  //执行业务查询的主方法
  override def run(ctx: SourceFunction.SourceContext[Demon]): Unit = {
    val rs: ResultSet = pst.executeQuery()
    while (rs.next()){
      val id: Int = rs.getInt(1)
      val name: String = rs.getString(2)
      val age: Int = rs.getInt(3)
      ctx.collect(Demon(id,name,age))
    }

  }

  //关流
  override def close(): Unit = {
    if(pst != null){
      pst.close()
    }
    if(conn != null){
      conn.close()
    }

  }

  override def cancel(): Unit = ???
}
