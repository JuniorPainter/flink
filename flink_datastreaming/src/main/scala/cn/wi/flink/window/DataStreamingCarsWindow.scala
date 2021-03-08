package cn.wi.flink.window

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingCarsWindow
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/2 16:28
 */
object DataStreamingCarsWindow {
  def main(args: Array[String]): Unit = {
    //TODO 获取流处理执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //TODO 加载数据源
    val dataDS: DataStream[String] = environment.socketTextStream("localhost", 9999)

    //TODO 数据转换
    val window: KeyedStream[DataStreamingCarsWindow, Tuple] = dataDS.map((line: String) => {
      val array: Array[String] = line.split(",")
      DataStreamingCarsWindow(array(0).toInt, array(1).toInt)
    })
      //分组 既可以用下标也可以用字段名
      .keyBy("id")

    //无重叠的时间窗口  每隔3s统计一次
    //    window
    //      .timeWindow(Time.seconds(3))
    //count 是值字段名
    //      .sum("count")
    //      .print()

    //有重叠的时间窗口 第一个参数：时间长度  第二:触发时间
    //每3s触发一次  计算过去6s内的数据 因为每隔3s统计一次  6s内的数据和 是上一个3s统计的值加这一个3s的数据
    //    window
    //      .timeWindow(Time.seconds(6),Time.seconds(3))
    //        .sum("count")
    //        .print()


    //无重叠的数量窗口   只有当一样的K的数据出现三次才会触发 否则不触发
    //    window.countWindow(3)
    //      .sum("count")
    //      .print()

    //有重叠的数量窗口 K出现三次才会触发一次  会计算过去出现6次的数据
    window.countWindow(6, 3)
      .sum(1)
      .print()
    environment.execute("cat sum")
  }
}

case class DataStreamingCarsWindow(id: Int, count: Int)