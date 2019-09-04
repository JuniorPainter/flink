package cn.wi.flink.source

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetReadCsvSource
 * @Author: xianlawei
 * @Description: 读取本地CSV文件
 * @date: 2019/9/2 8:33
 */
object DataSetReadCsvSource {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataDS: DataSet[(String, String, Int)] = environment.readCsvFile[(String, String, Int)](
      filePath = "D:\\Flink\\input\\test\\t2.csv",
      //换行符
      lineDelimiter = "\n",
      //字符间隔
      fieldDelimiter = ",",
      //是否忽略第一行
      ignoreFirstLine = true,
      //应该读取的文件中的字段。默认情况下所有字段
      includedFields = Array(0, 1, 2)
    )
    dataDS
      //如果与分组联用   会将每组的前两个输出
      .groupBy(1)
      .first(2)
      .print()
  }
}
