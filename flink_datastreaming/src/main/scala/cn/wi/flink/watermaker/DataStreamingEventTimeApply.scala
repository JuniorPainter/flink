package cn.wi.flink.watermaker

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingEventTimeApply
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 12:00
 */
object DataStreamingEventTimeApply {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val dataDS: DataStream[String] = environment.socketTextStream("localhost", 9999)

    //设置事件时间  指定那种类型的时间 EventTime
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //数据转换，新建Bean
    val bossDS: DataStream[Boss] = dataDS
      .map(line => {
        val array: Array[String] = line.split(",")
        Boss(array(0).toLong, array(1), array(2), array(3).toDouble)
      })

    //提取时间
    bossDS.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[Boss] {
        val delayTime: Long = 2000L

        var currentTimeStamp: Long = 0L

        override def getCurrentWatermark: Watermark = {
          new Watermark(currentTimeStamp - delayTime)
        }

        override def extractTimestamp(element: Boss, previousElementTimestamp: Long): Long = {
          val timestamp: Long = element.timestamp
          currentTimeStamp = Math.max(timestamp, currentTimeStamp)
          timestamp
        }
      })
      //分组
      .keyBy(line => line.boss)
      //划分时间窗口
      .timeWindow(Time.seconds(3))
      //apply 处理复杂的业务函数
      .apply(new MyMaxFunction)
      .print()
    environment.execute()

  }
}
