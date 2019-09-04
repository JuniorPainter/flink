package cn.wi.flink.mysql

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingMySQL
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 13:55
 */
object DataStreamingMySQL {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // val dataDS: DataStream[Demon] = environment.fromElements(Demon(10, "XiaoLi", 20))

    //RichSourceFunction 继承了AbstractRichFunction方法 可以复写下面的多个方法
    val dataDS: DataStream[Demon] = environment.addSource(new RichSourceFunction[Demon] {

      var connection: Connection = null
      var pst: PreparedStatement = null

      //数据预处理方法 初始化方法 在run方法之前执行
      override def open(parameters: Configuration): Unit = {
        //建立连接，配置MySQL属性
        val driver: String = "com.mysql.jdbc.Driver"
        val userName: String = "root"
        val password: String = "123456"
        val url: String = "jdbc:mysql://localhost:3306/test"

        //加载驱动
        Class.forName(driver)
        connection = DriverManager.getConnection(url, userName, password)

        pst = connection.prepareStatement("SELECT * FROM demon")

      }

      override def run(ctx: SourceFunction.SourceContext[Demon]): Unit = {
        val result: ResultSet = pst.executeQuery()
        while (result.next()) {
          val id: Int = result.getInt("id")
          val name: String = result.getString("name")
          val age: Int = result.getInt("age")
          //收集数据：输出结果
          ctx.collect(Demon(id, name, age))
        }
      }

      //关闭操作
      override def close(): Unit = {
        if (pst != null) {
          pst.close()
        }
        if (connection != null) {
          connection.close()
        }
      }

      override def cancel(): Unit = ???
    })

    dataDS.print()


    environment.execute()
  }
}
