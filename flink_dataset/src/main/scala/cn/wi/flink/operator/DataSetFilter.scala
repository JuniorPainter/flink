package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetFilter
 * @Author: xianlawei
 * @Description: 过滤出带java的数据
 * @date: 2019/9/1 16:32
 */
object DataSetFilter {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[String] = environment.fromElements("java", "scala", "java")


    dataDS.filter((word: String) =>
      word.contains("java")
    ).print()
  }
}
