package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetTopN
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 22:50
 */
object DataSetTopN {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataML: mutable.MutableList[(Int, Long, String)] = new mutable.MutableList[(Int, Long, String)]
    dataML.+=((1, 1L, "Hi"))
    dataML.+=((2, 2L, "Hello"))
    dataML.+=((3, 2L, "Hello world"))
    dataML.+=((4, 3L, "Hello world, how are you?"))
    dataML.+=((5, 3L, "I am fine."))
    dataML.+=((6, 3L, "Luke Skywalker"))
    dataML.+=((7, 4L, "Comment#1"))
    dataML.+=((8, 4L, "Comment#2"))
    dataML.+=((9, 4L, "Comment#3"))
    dataML.+=((10, 4L, "Comment#4"))
    dataML.+=((11, 5L, "Comment#5"))
    dataML.+=((12, 5L, "Comment#6"))
    dataML.+=((13, 5L, "Comment#7"))
    dataML.+=((14, 5L, "Comment#8"))
    dataML.+=((15, 5L, "Comment#9"))
    dataML.+=((16, 6L, "Comment#10"))
    dataML.+=((17, 6L, "Comment#11"))
    dataML.+=((18, 6L, "Comment#12"))
    dataML.+=((19, 6L, "Comment#13"))
    dataML.+=((20, 6L, "Comment#14"))
    dataML.+=((21, 6L, "Comment#15"))

    val dataDS: DataSet[(Int, Long, String)] = environment.fromCollection(dataML)
    //first,取值,下标是从1开始的
    dataDS.first(2).print()
  }
}
