package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetReduce
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 16:35
 */
object DataSetReduce {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[(String, Int)] = environment.fromElements(("spark", 1), ("scala", 1), ("spark", 1))

    dataDS
      //这里输入的数据直接就是元组 直接按照dataDS的每一个元素取出操作即可
      .map((line: (String, Int)) => line)
      //0表示第一个字段
      .groupBy(0)
      //.groupBy(_._1)  _1表示元组中的第一个元素
      //x,y都分别表示一个元组  上面分组按照第一个字段进行分组，则组内所有元组的第一个元素都是相同的，将第二个值相加
      .reduce((x, y) => (x._1, x._2 + y._2))
      .print()
  }
}
