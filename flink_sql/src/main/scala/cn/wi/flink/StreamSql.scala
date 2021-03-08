package cn.wi.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row

/**
  * @Date 2019/10/20
  */
object StreamSql {

  def main(args: Array[String]): Unit = {
    /**
      * 1)获取流处理运行环境
      * 2)获取Table运行环境
      * 3)设置处理时间为 EventTime
      * 4)创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
      * 5)创建一个自定义数据源
      * 6)添加水印，允许延迟2秒
      * 7)导入 import org.apache.flink.table.api.scala._ 隐式参数
      * 8)使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
      * 9)编写SQL语句统计用户订单总数、最大金额、最小金额
      * 分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
      * 10)使用 tableEnv.sqlQuery 执行sql语句
      * 11)将SQL的执行结果转换成DataStream再打印出来
      * 12)启动流处理程序
      * 需求：使用Flink SQL来统计5秒内 用户的 订单总数、订单的最大金额、订单的最小金额。
      */

    //1)获取流处理运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //2)获取Table运行环境
    val tblEnv: StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)
    //3)设置处理时间为 EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //5)创建一个自定义数据源
    val source: DataStream[Order] = env.addSource(new OrderSourceFunc)
    //6)添加水印，允许延迟2秒
    val waterData: DataStream[Order] = source.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(2)) {
      override def extractTimestamp(element: Order): Long = {
        val time: Long = element.orderTime
        time
      }
    })

    //7)导入 import org.apache.flink.table.api.scala._ 隐式参数
    import org.apache.flink.table.api.scala._
    //8)使用 registerDataStream 注册表，并分别指定字段，还要指定rowtime字段
    tblEnv.registerDataStream("Order",waterData,'orderId,'userId,'orderPrice,'orderTime.rowtime)

    // 9)编写SQL语句统计用户订单总数、最大金额、最小金额
    //      * 分组时要使用 tumble(时间列, interval '窗口时间' second) 来创建窗口
    val sql = "select userId,count(orderId),max(orderPrice),min(orderPrice) from Order group by userId,tumble(orderTime, interval '5' second) "

    //10)使用 tableEnv.sqlQuery 执行sql语句
    val table: Table = tblEnv.sqlQuery(sql)

    //11)将SQL的执行结果转换成DataStream再打印出来
    val values: DataStream[Row] = tblEnv.toAppendStream[Row](table)
    values.print()

    //12)启动流处理程序
    env.execute()
  }
}


