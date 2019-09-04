package cn.wi.flink.table

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.CsvTableSource

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: TableSourceAndSink
 * @Author: xianlawei
 * @Description: 使用tableSource和tableSink读写数据
 * @date: 2019/9/3 15:14
 */
object TableSourceAndSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: StreamTableEnvironment = TableEnvironment.getTableEnvironment(environment)

    val tableSource: CsvTableSource = CsvTableSource.builder()
      .path("D:\\Flink\\input\\test\\t2.csv")
      .field("name", Types.STRING)
      .field("address", Types.STRING)
      .field("age", Types.INT)
      //行分隔符
      .lineDelimiter("\n")
      //每一行的每个字段之间的分割符
      .fieldDelimiter(",")
      //忽略首行
      .ignoreFirstLine()
      .build()

    //将外部数据构建成表
    tableEnvironment.registerTableSource("TableSourceAndSink", tableSource)

    //使用table api/sql 查询

    // 使用table api查询
    val table: Table = tableEnvironment
      .scan("TableSourceAndSink")
      .select("name,age")
      .filter("age>20")

    //table api 无法直接打印   先通过sink落地 进行操作
    table.writeToSink(new CsvTableSink(
      "D:\\Flink\\output\\TableSourceAndSink\\",
      //每一行每个字段的分隔符
      ",",
      //生成文件的数量
      1,
      WriteMode.OVERWRITE
    ))

    environment.execute()
  }
}
