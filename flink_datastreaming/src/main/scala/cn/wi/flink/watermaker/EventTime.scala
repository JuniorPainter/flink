package cn.wi.flink.watermaker

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: EventTime
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 10:07
 */
object EventTime {
  def main(args: Array[String]): Unit = {

    //获取执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    //加载数据源
    val dataDS: DataStream[String] = environment.socketTextStream("localhost", 9999)

    //设置事件时间
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)



    //数据转换，新建Bean
    dataDS.map(line => {
      val array: Array[String] = line.split(",")
      Boss(array(0).toLong, array(1), array(2), array(3).toDouble)
    })
      //提取消息源时间
      //1527911155000,boos1,pc1,100.0
      //.assignAscendingTimestamps(line=>line.timestamp)
      .assignAscendingTimestamps(_.timestamp)
      .keyBy(line => line.boss)
      //3s内
      .timeWindow(Time.seconds(3))
      .maxBy("price")
      .print()

    environment.execute()
  }


}
