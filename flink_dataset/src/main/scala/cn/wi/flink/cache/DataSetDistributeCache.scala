package cn.wi.flink.cache

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
 * @ProjectName: Flink_Parent
 * @ClassName: DataSetDistributeCache
 * @Author: xianlawei
 * @Description: 从HDFS上拿到subject.txt数据，再与Clazz数据组合成新的数据集，组成：(学号，班级，学科，分数)
 * @date: 2019/9/2 10:13
 */
object DataSetDistributeCache {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //开启分布式缓存
    environment.registerCachedFile("hdfs://node01:8020/flink/input/subject.txt", "cache")

    val dataClazz: DataSet[Clazz] = environment.fromElements(
      Clazz(1, "class_1"),
      Clazz(2, "class_2"),
      Clazz(3, "class_3"),
      Clazz(4, "class_4"),
      Clazz(5, "class_5"),
      Clazz(6, "class_6"),
      Clazz(7, "class_7"),
      Clazz(8, "class_8")
    )

    dataClazz.map(new RichMapFunction[Clazz, Info] {
      //将封装了数据的数组赋值给buffer
      val buffer = new ArrayBuffer[String]()

      override def open(parameters: Configuration): Unit = {
        //获取上面文件  用缓存的别名
        val cache: File = getRuntimeContext.getDistributedCache.getFile("cache")

        val lines: Iterator[String] = Source
          //获取绝对路径
          .fromFile(cache.getAbsoluteFile)
          //获取每一行的内容
          .getLines()
        lines.foreach(line => {
          buffer.append(line)
        })
      }

      override def map(value: Clazz): Info = {
        var info: Info = null
        for (line <- buffer) {
          val array: Array[String] = line.split(",")
          if (array(0).toInt == value.id) {
            //用INFO进行封装
            info = Info(value.id, value.clazz, array(1), array(2).toDouble)
          }
        }
        info
      }
    }).print()


  }
}
