package cn.wi.flink.kafka

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[Demon] = executionEnvironment.fromElements(Demon(1, "xiao", 20))
    val sourceDataStream: DataStream[String] = source.map((line: Demon) => line.toString)

    //flink整合kafka
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")

    val kafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String]("demo",
      new SimpleStringSchema(),properties)

    //数据写入kafka
    sourceDataStream.addSink(kafkaProducer)

    //触发执行
    executionEnvironment.execute()
  }
}
