package cn.wi.flink.operator

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetRebalance
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 22:11
 */
object DataSetRebalance {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //生成序列数据源   随机创建并行数字
    val dataDS: DataSet[Long] = environment.generateSequence(0, 100)

    dataDS
      .filter(_ > 50)
      .rebalance()
      //map数据转换Long->(subtask,Long)
      .map(
        // 使用map操作传入 RichMapFunction ，将当前子任务的ID和数字构建成一个元组  转换数据格式
        new RichMapFunction[Long, (Int, Long)] {
          var subtask = 0

          //数据预处理和初始化
          override def open(parameters: Configuration): Unit = {
            //通过getRuntimeContext上下文对象，获获取线程任务执行id
            subtask = getRuntimeContext.getIndexOfThisSubtask
          }

          //正常业务处理  数据转换
          override def map(value: Long): (Int, Long) = {
            (subtask, value)
          }
        }).print()
  }
}
