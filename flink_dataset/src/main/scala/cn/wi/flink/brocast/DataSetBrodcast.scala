package cn.wi.flink.brocast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
 * @ProjectName: Flink_Parent
 * @ClassName: DataSetBrodcast
 * @Author: xianlawei
 * @Description: 从内存中拿到data3的广播数据，再与data2数据根据第二列元素组合成(Int, Long, String, String)
 * @date: 2019/9/2 9:48
 */
object DataSetBrodcast {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataML01: mutable.MutableList[(Int, Long, String)] = new mutable.MutableList[(Int, Long, String)]
    dataML01.+=((1, 1L, "xiaoming"))
    dataML01.+=((2, 2L, "xiaoli"))
    dataML01.+=((3, 2L, "xiaoqiang"))
    val dataDS01: DataSet[(Int, Long, String)] = environment.fromCollection(dataML01)

    val dataML02: mutable.MutableList[(Int, Long, Int, String, Long)] = new mutable.MutableList[(Int, Long, Int, String, Long)]
    dataML02.+=((1, 1L, 0, "Hallo", 1L))
    dataML02.+=((2, 2L, 1, "Hallo Welt", 2L))
    dataML02.+=((2, 3L, 2, "Hallo Welt wie", 1L))
    val dataDS02: DataSet[(Int, Long, Int, String, Long)] = environment.fromCollection(dataML02)

    //将dataDS02广播到内存中
    //RichMapFunction<IN, OUT>
    //.map是对输入的进行转换，所以dataDS01是输入量，(Int, Long, String)与dataSet01的参数类型是对应的  表示输入In
    //(Int, Long, String, String)与输出的类型是对应的   表示输出OUT
    //匿名函数
    dataDS01.map(
      // 使用map操作传入 RichMapFunction ，将当前子任务的ID和数字构建成一个元组  转换数据格式
      //RichMapFunction[(输入), (输出)]
      new RichMapFunction[(Int, Long, String), (Int, Long, String, String)] {
        var buffer: mutable.Buffer[(Int, Long, Int, String, Long)] = _

        import collection.JavaConverters._

        //open在map方法之前先执行
        override def open(parameters: Configuration): Unit = {
          //getRuntimeContext 获取上下文对象 通过上下文对象获取广播变量
          val dataUL: util.List[(Int, Long, Int, String, Long)] =
          //dataDS02的变量数量集和类型  获取广播变量
            getRuntimeContext.getBroadcastVariable[(Int, Long, Int, String, Long)]("dataDS02")
          //将Java数据转换成Scala数据格式
          buffer = dataUL.asScala
        }

        //数据转换，负责转换成四元组(Int, Long, String, String)
        override def map(value: (Int, Long, String)): (Int, Long, String, String) = {
          var tuple: (Int, Long, String, String) = null
          //对广播变量进行循环  获取广播变量的每一条数据
          for (line <- buffer) {
            if (line._2 == value._2) {
              //进行数据合并
              tuple = (value._1, value._2, value._3, line._4)
            }
          }
          tuple
        }
      })
      //共享广播变量  与open中别名一致
      .withBroadcastSet(dataDS02, "dataDS02").print()
  }
}
