package cn.wi.flink.table


import cn.wi.flink.table.bean.Project
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: TableToDataSet
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 15:07
 */
object TableToDataSet {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: BatchTableEnvironment = TableEnvironment.getTableEnvironment(environment)

    //2. 构建数据源
    val data = List(
      Project(1L, 1, "Hello"),
      Project(2L, 2, "Hello"),
      Project(3L, 3, "Hello"),
      Project(4L, 4, "Hello"),
      Project(5L, 5, "Hello"),
      Project(6L, 6, "Hello"),
      Project(7L, 7, "Hello World"),
      Project(8L, 8, "Hello World"),
      Project(8L, 8, "Hello World"),
      Project(20L, 20, "Hello World"))
    val source: DataSet[Project] = environment.fromCollection(data)
    //3. 把流转换为表
    val table: Table = tableEnvironment.fromDataSet(source)

    //4. 将表再转换成流
    val value: DataSet[Project] = tableEnvironment.toDataSet[Project](table)

    //5.数据打印,print是一个action算子，能够触发执行
    value.print()
  }
}
