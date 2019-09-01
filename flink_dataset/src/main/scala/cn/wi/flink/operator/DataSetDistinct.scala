package cn.wi.flink.operator

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetDistinct
 * @Author: xianlawei
 * @Description: 数据去重
 * @date: 2019/9/1 21:28
 */
object DataSetDistinct {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataML: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int,String,Double)]
    dataML.+=((1, "Chinese", 90.0))
    dataML.+=((2, "Math", 20.0))
    dataML.+=((3, "English", 30.0))
    dataML.+=((4, "Physical", 40.0))
    dataML.+=((5, "Chinese", 50.0))
    dataML.+=((6, "Physical", 60.0))
    dataML.+=((7, "Chinese", 70.0))

    val dataDS: DataSet[(Int, String, Double)] = environment.fromCollection(dataML)

    //按照字段进行去重 0 1 2
    dataDS.distinct(1).print()
  }
}
