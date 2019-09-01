package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetMap
 * @Author: xianlawei
 * @Description: map 将DataSet中每一元素转换成另外一个元素
 *               使用map操作，将以下数据
 *               List("1,张三", "2,李四", "3,王五", "4,赵六")
 *               转换为一个scala的样例类。
 * @date: 2019/9/1 11:51
 */
object DataSetMap {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //使用 fromCollection 构建数据源
    val mapDS: DataSet[String] = environment.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))

    //使用map操作执行转换
    mapDS.map(line => {
      val field: Array[String] = line.split(",")
      DataSetMap(field(0), field(1))
    }
    ).print()

  }
}

case class DataSetMap(id: String, name: String)