package cn.wi.flink.operator

import java.lang

import org.apache.flink.api.common.functions.GroupCombineFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetCombineGroup
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 17:07
 */
object DataSetCombineGroup {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[(String, Int)] = environment
      .fromElements(("flink", 1), ("scala", 1), ("hadoop", 1), ("flink", 1), ("spark", 1), ("hadoop", 1))

    dataDS
      .groupBy(_._1)
      .combineGroup(new DataSetCombineGroup)
      .print()
  }
}

import collection.JavaConverters._

//提前减少数据量
class DataSetCombineGroup extends GroupCombineFunction[(String, Int), (String, Int)] {
  override def combine(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    var key = ""
    var count = 0
    for (line <- values.asScala) {
      key = line._1
      count = line._2 + count
    }
    out.collect((key, count))
  }
}