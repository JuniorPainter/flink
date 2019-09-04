package cn.wi.flink.table

import cn.wi.flink.table.bean.Order
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sinks.CsvTableSink

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingToTable
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 15:03
 */
object DataStreamingToTable {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: StreamTableEnvironment = TableEnvironment.getTableEnvironment(environment)

    val dataDS01: DataStream[Order] = environment.fromElements(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)
    )

    val dataDS02: DataStream[Order] = environment.fromElements(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)
    )

    //把流转换成表
    tableEnvironment.registerDataStream("dataDS01", dataDS01)
    tableEnvironment.registerDataStream("dataDS02", dataDS02)

    //使用SQL来查询
    val table: Table = tableEnvironment
      .sqlQuery("select * from dataDS01 union all select * from dataDS02")

    //数据落地
    table.writeToSink(new CsvTableSink(
      "D:\\Flink\\output\\table\\DataStreamingToTable\\",
      ",",
      1,
      WriteMode.OVERWRITE))

    environment.execute()
  }
}
