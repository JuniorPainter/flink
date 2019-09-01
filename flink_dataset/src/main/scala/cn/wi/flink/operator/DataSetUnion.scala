package cn.wi.flink.operator

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetUnion
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 22:01
 */
object DataSetUnion {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS01: DataSet[String] = environment.fromElements("java")
    val dataDS02: DataSet[String] = environment.fromElements("java")
    val dataDS03: DataSet[String] = environment.fromElements("scala")

    dataDS01.union(dataDS02).union(dataDS03).print()
  }
}
