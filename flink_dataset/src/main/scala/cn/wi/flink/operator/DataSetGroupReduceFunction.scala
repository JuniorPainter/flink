package cn.wi.flink.operator

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetGroupReduceFunction
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 17:34
 */
object DataSetGroupReduceFunction {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[(String, Int)] = environment
      .fromElements(("spark", 1), ("flink", 1), ("scala", 1), ("hadoop", 1), ("flink", 1), ("spark", 1), ("hadoop", 1))

    dataDS
      .groupBy(_._1)
      //自定义匿名函数，来进行数据转换
      .combineGroup(new DataSetGroupReduceFunction)
      .print()
  }
}

import collection.JavaConverters._

class DataSetGroupReduceFunction
  extends GroupReduceFunction[(String, Int), (String, Int)]
    with GroupCombineFunction[(String, Int), (String, Int)] {

  //reduce后执行
  override def reduce(values: lang.Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
    for (line <- values.asScala) {
      out.collect(line)
    }
  }

  //combine先执行
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