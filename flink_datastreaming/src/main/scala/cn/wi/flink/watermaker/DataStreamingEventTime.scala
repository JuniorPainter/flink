package cn.wi.flink.watermaker

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingEventTime
 * @Author: xianlawei
 * @Description: 以EventTime划分窗口，计算3秒钟内出价最高的产品
 * @date: 2019/9/2 21:07
 */
object DataStreamingEventTime {
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
      //周期性的触发水位线
      new AssignerWithPeriodicWatermarks[Boss] {
        //延迟时间 这是延迟窗口  表示在当前时间currentTimeStamp+2s的时间内都可以包含接收？
        //延迟时间根据实际业务延迟时间
        //延迟时间几秒，就会自动划分几条数据 作为第一个窗口
        val delayTime: Long = 2000L

        var currentTimeStamp: Long = 0L

        //再获取水平线  当前时间-延迟时间  接收数据的时间又延迟几秒
        override def getCurrentWatermark: Watermark = {
          new Watermark(currentTimeStamp - delayTime)
        }

        override def extractTimestamp(element: Boss, previousElementTimestamp: Long): Long = {
          val timestamp: Long = element.timestamp
          //求最大值  谁大求谁
          //第一条消息传进来时 currentTime是0 则最大的时间就是传进来的数据的时间  当前时间就和最大时间同步
          //当后续数据进来时，后续进来的时间比当前时间大，则后续消息的时间会赋值currentTimeStamp
          // 使得currentTimeStamp  的时间一直是最新的时间
          //加入后续消息存在时间比currentTimeStamp小的(可能是网络延迟造成的)，currentTimeStamp时间不变
          // 保证了时间轴一直向前走 不会倒退
          currentTimeStamp = Math.max(timestamp, currentTimeStamp)
          timestamp
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