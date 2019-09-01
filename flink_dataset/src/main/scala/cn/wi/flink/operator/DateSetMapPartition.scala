package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DateSetMapPartition
 * @Author: xianlawei
 * @Description: MapPartition
 *               使用mapPartition操作，将以下元组数据
 *               ("java", 1), ("scala", 1), ("java", 1)
 *               转换为一个scala的样例类。
 * @date: 2019/9/1 11:51
 */
object DateSetMapPartition {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val tupleDS: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("java", 1))

    tupleDS.mapPartition(iter =>
      iter.map(line =>
        //直接转成二元组
        (line._1, line._2)
        // 转成样例类
        // DateSetMapPartition(line._1, line._2)
      )
    ).print()
  }
}

case class DateSetMapPartition(name: String, count: Int)
