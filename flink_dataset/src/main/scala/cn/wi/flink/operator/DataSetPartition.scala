package cn.wi.flink.operator

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetPartition
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 22:24
 */
object DataSetPartition {
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
    //partitionByHash
        dataDS
          // 0 1 2 1表示第二个参数
          .partitionByHash(1)
          .mapPartition(line =>
            line.map(x =>
              (x._1, x._2, x._3)
            )
          ).writeAsText("D:\\Flink\\output\\partition\\partitionByHash", WriteMode.OVERWRITE)


    //rangePartition

    //下标1 2 3
    dataDS.partitionByRange(x => x._2)
      .mapPartition(line =>
        line.map(x => (x._1, x._2, x._3)
        )
      ).writeAsText("D:\\Flink\\output\\partition\\rangePartition", WriteMode.OVERWRITE)
    environment.execute()

    dataDS.map(line => line)
      //设置并行度
      .setParallelism(2)
      //第一个参数表示按照哪个字段进行分区 0 1 2
      .sortPartition(1, Order.DESCENDING)
      .mapPartition(line => line.map(x => (x._1, x._2, x._3)
      )
      ).writeAsText("D:\\Flink\\output\\partition\\sortPartition", WriteMode.OVERWRITE)
    environment.execute()
  }
}
