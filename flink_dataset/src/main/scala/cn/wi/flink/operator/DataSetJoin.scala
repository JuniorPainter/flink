package cn.wi.flink.operator

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetJoin
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 21:39
 */
object DataSetJoin {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataML: mutable.MutableList[(Int, String, Double)] = new mutable.MutableList[(Int, String, Double)]
    //学号--学科--分数
    dataML.+=((1, "Chinese", 90.0))
    dataML.+=((2, "Math", 20.0))
    dataML.+=((3, "English", 30.0))
    dataML.+=((4, "Physical", 40.0))
    dataML.+=((5, "Chinese", 50.0))
    dataML.+=((6, "Physical", 60.0))
    dataML.+=((7, "Chinese", 70.0))
    val dataDS01: DataSet[(Int, String, Double)] = environment.fromCollection(dataML)

    val dataMT: mutable.MutableList[(Int, String)] = new mutable.MutableList[(Int, String)]
    //学号--班级
    dataMT.+=((1, "class_1"))
    dataMT.+=((2, "class_1"))
    dataMT.+=((3, "class_2"))
    dataMT.+=((4, "class_2"))
    dataMT.+=((5, "class_3"))
    dataMT.+=((6, "class_3"))
    dataMT.+=((7, "class_4"))
    dataMT.+=((8, "class_1"))
    val dataDS02: DataSet[(Int, String)] = environment.fromCollection(dataMT)

    //Join操作
    dataDS01
      .join(dataDS02)
      //左边数据集
      .where(0)
      //右边数据集
      .equalTo(0) {
            //组合操作
        (dataDS01, dataDS02) => (dataDS02._1, dataDS02._2, dataDS01._2, dataDS01._3)
      }
      //按照join左边的字段进行分组
      .groupBy(1, 2)

      .maxBy(3)
      .print()
  }
}
