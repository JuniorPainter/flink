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
      .map(new RichMapFunction[Long, (Int, Long)] {
        var subtask = 0

        override def open(parameters: Configuration): Unit = {
          //通过上下文对象，获取任务/线程ID
          subtask = getRuntimeContext.getIndexOfThisSubtask
        }

        override def map(value: Long): (Int, Long) = {
          (subtask, value)
        }
      }).print()
  }
}
