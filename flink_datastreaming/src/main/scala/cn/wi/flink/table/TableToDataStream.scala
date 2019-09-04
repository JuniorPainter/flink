package cn.wi.flink.table

import cn.wi.flink.table.bean.Project
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.types.Row

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: TableToDataStream
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 15:11
 */
object TableToDataStream {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: StreamTableEnvironment = TableEnvironment.getTableEnvironment(environment)

    //构建数据源
    val dataList: List[Project] = List(
      Project(1L, 1, "Hello"),
      Project(2L, 2, "Hello"),
      Project(3L, 3, "Hello"),
      Project(4L, 4, "Hello"),
      Project(5L, 5, "Hello"),
      Project(6L, 6, "Hello"),
      Project(7L, 7, "Hello World"),
      Project(8L, 8, "Hello World"),
      Project(8L, 8, "Hello World"),
      Project(20L, 20, "Hello World")
    )

    val dataDS: DataStream[Project] = environment.fromCollection(dataList)

    //将流转换成表
    val table: Table = tableEnvironment.fromDataStream(dataDS)

    //Project 返回类型  必须给
    val value: DataStream[Project] = tableEnvironment.toAppendStream[Project](table)

    //retract 如果boolean值为true表示添加，如果为false，表示撤销
    val valueDS: DataStream[(Boolean, Project)] = tableEnvironment.toRetractStream[Project](table)
    //如果不知道类型  可以直接给Row
   // val valueDS: DataStream[(Boolean, Row)] = tableEnvironment.toRetractStream[Row](table)


    valueDS.print()
    value.print()
    environment.execute()
  }
}
