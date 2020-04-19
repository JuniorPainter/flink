package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetReduceGroup
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 16:47
 */
object DataSetReduceGroup {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[(String, Int)] = environment.fromElements(("java", 1), ("scala", 1), ("hadoop", 1), ("java", 1), ("spark", 1), ("hadoop", 1))

    dataDS
      .groupBy(_._1)
      //
      .reduceGroup(
        //in 表示输入的数据 分组的数据集  out是输出的数据
        (in: Iterator[(String, Int)], out: Collector[(String, Int)]) => {
          //将输入的数据(分组的数据集)进行reduce聚合
          val tuple: (String, Int) = in.reduce((x, y) => (x._1, x._2 + y._2))
          //收集输出出去
          out.collect(tuple)
        }
      ).print()
  }
}
