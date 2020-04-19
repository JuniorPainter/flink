package cn.wi.flink.operator

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetAggregate
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 21:09
 */
object DataSetAggregate {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataML: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int, String, Double)]
    dataML.+=((1, "Chinese", 90.0))
    dataML.+=((2, "Math", 20.0))
    dataML.+=((3, "English", 30.0))
    dataML.+=((4, "Physical", 40.0))
    dataML.+=((5, "Chinese", 50.0))
    dataML.+=((6, "Physical", 60.0))
    dataML.+=((7, "Chinese", 70.0))

    val dataDS: DataSet[(Int, String, Double)] = environment.fromCollection(dataML)

    dataDS
      .groupBy(1)
      //单独使用的时候，输出每组的最小或者最大值  2表示第三个元素 根据第三个元素取最小值
      .minBy(2)
      //返回满足条件的一组元素
      //.maxBy(2)
      //返回满足条件的最值
      //.max(2)
      //单独使用的时候，输出每组的最大值
      //两者合用的时候 例如：会从每组最小的集合中 按照第二个字段选出最大的
      .aggregate(Aggregations.MAX,2)
      // 每组最小值 (4,Physical,40.0)       组合使用：返回结果(5,Chinese,50.0)
      //  (5,Chinese,50.0)
      //  (3,English,30.0)
      //  (2,Math,20.0)
      .print()
  }
}
