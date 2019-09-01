package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetFlatMap
 * @Author: xianlawei
 * @Description: flatMap
 *               使用flatMap操作，将集合中的数据
 *               List(("java", 1), ("scala", 1), ("java", 1))
 *               根据第一个元素，进行分组
 *               根据第二个元素，进行聚合求值
 * @date: 2019/9/1 16:08
 */
object DataSetFlatMap {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val flatMapDS: DataSet[List[(String, Int)]] = environment.fromElements(List(("java", 1), ("scala", 1), ("java", 1)))

    // val tupleDS: DataSet[(String, Int)] = flatMapDS.flatMap(line=>line)
    flatMapDS
      //只是单纯将元素取出
      .flatMap(line => line)
      .groupBy(0)
      .sum(1)
      .print()
  }
}
