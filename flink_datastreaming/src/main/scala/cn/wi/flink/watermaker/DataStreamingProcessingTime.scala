package cn.wi.flink.watermaker

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingProcessingTime
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 11:53
 */
object DataStreamingProcessingTime {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val dataDS: DataStream[String] = environment.socketTextStream("localhost", 9999)

    //设置事件时间  指定那种类型的时间 EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    //数据转换，新建Bean
    val bossDS: DataStream[Boss] = dataDS
      .map(line => {
        val array: Array[String] = line.split(",")
        Boss(array(0).toLong, array(1), array(2), array(3).toDouble)
      })

    //提取时间 ProcessingTime是系统时间  直接是系统时间即可
    bossDS.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[Boss] {

        //再获取水平线  当前时间-延迟时间  接收数据的时间又延迟几秒
        override def getCurrentWatermark: Watermark = {
          new Watermark(System.currentTimeMillis())
        }

        override def extractTimestamp(element: Boss, previousElementTimestamp: Long): Long = {
          System.currentTimeMillis()
        }
      })
      //分组
      .keyBy(line => line.boss)
      //划分时间窗口
      .timeWindow(Time.seconds(3))
      //.apply(new MyMaxFunction)
      .maxBy("price")
      .print()
    environment.execute()

  }
}
