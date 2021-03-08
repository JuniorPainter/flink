package cn.wi.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamMySQLSink
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 14:27
 */
object DataStreamMySQLSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //封装数据(Bean)
    val demon: DataStream[Demon] = environment.fromElements(Demon(10, "xiaoli", 20))


    //进行数据转换，自定义
    demon.addSink(new RichSinkFunction[Demon] {
      var connection: Connection = null
      var preparedStatement:PreparedStatement=null

      //先执行预处理
      override def open(parameters: Configuration): Unit = {
        val driver = "com.mysql.jdbc.Driver"
        val userName = "root"
        val password = "123456"
        val url ="jdbc:mysql://localhost:3306/test"
        Class.forName(driver)
        connection = DriverManager.getConnection(url,userName,password)
        preparedStatement = connection.prepareStatement("insert into demon value (?,?,?)")
      }

      //再处理数据
      override def invoke(value: Demon): Unit = {
        //下标从1开始
        preparedStatement.setInt(1,value.id)
        preparedStatement.setString(2,value.name)
        preparedStatement.setInt(3,value.age)
        preparedStatement.executeUpdate()
      }

      //最后关流
      override def close(): Unit = {
        if (preparedStatement!=null){
          preparedStatement.close()
        }
        if (connection!=null){
          connection.close()
        }
      }
    })
    environment.execute()
  }
}
