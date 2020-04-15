package cn.wi.flink.wordcount

import java.util.Date

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/15
 */
object WordCountReview {
  def main(args: Array[String]): Unit = {
    //获取Flink批处理的执行环境
    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //加载数据源
    val sourceDataSet: DataSet[String] = executionEnvironment.fromElements("ss dd dd ss ff")
    //数据转换：切分，分组，聚合
    sourceDataSet
      .flatMap((line: String) => line.split("\\W+"))
      //(ss,1)
      .map((line: String) => (line, 1))
      //对元组第一个元素进行分组
      .groupBy(0)
      //对元组第二个元组进行聚合
      .sum(1)
      //数据打印 在批处理中 print是一个触发算法  有这个就不需要写executionEnvironment.execute()
      .print()

    executionEnvironment.execute()
  }
}
