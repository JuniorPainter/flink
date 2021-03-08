package cn.wi.flink.table

import cn.wi.flink.table.bean.Order
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sinks.CsvTableSink

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetTable
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 14:56
 */
object DataSetTable {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //批的执行环境
    val tableEnvironment: BatchTableEnvironment = TableEnvironment.getTableEnvironment(environment)
    val dataDS01: DataSet[Order] = environment.fromElements(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2)
    )

    val dataDS02: DataSet[Order] = environment.fromElements(
      Order(2L, "pen", 3),
      Order(2L, "rubber", 3),
      Order(4L, "beer", 1)
    )

    //把
    tableEnvironment.registerDataSet("dataDS01",dataDS01)
    tableEnvironment.registerDataSet("dataDS02",dataDS02)

    val table: Table = tableEnvironment
      .sqlQuery("select * from dataDS01 union all select * from dataDS02")

    print(table.toString())


    table.writeToSink(new
        CsvTableSink("D:\\Flink\\output\\table\\DataStreamingToTable\\",",",1,WriteMode.OVERWRITE))

    environment.execute()
  }
}
