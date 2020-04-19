package cn.wi.flink.count

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 累加器
 * @Date: 2020/4/18
 */
object AccumulatorCount {
  def main(args: Array[String]): Unit = {
    /**
     * 1.获取执行环境
     * 2.加载数据源
     * 3.数据转换
     * （1）新建累加器
     * （2）注册累加器
     * （3）使用累加器
     * 4.批量数据sink
     * 5.触发执行
     * 6.获取累加器的结果
     */

    //获取执行环境
    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //加载数据源
    val source: DataSet[Int] = executionEnvironment.fromElements(1, 2, 3, 4, 5, 6)

    //数据转换
    val result: DataSet[Int] = source.map(
      new RichMapFunction[(Int), (Int)] {
        var intCounter = new IntCounter()

        override def open(parameters: Configuration): Unit = {
          //注册累加器
          getRuntimeContext.addAccumulator("accumulator", intCounter)
        }

        override def map(value: Int):Int = {
          //使用累加器
          intCounter.add(value)
          value
        }
      })

    //批量数据sink
    result.writeAsText("accumulator")

    //触发执行
    val executionResult: JobExecutionResult = executionEnvironment.execute()

    //获取累加器的结果
    val countResult: Int = executionResult.getAccumulatorResult[Int]("accumulator")
    println("累加器结果：" + countResult)
  }
}
