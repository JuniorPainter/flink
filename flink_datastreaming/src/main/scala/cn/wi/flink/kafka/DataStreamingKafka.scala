package cn.wi.flink.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer09, FlinkKafkaProducer09}
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingKafka
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 13:28
 */
object DataStreamingKafka {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //添加配置
    val properties: Properties = new Properties()

    //broker地址
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    //zk地址
    properties.setProperty("zookeeper.connect", "node01:2181,node02:2181,node03:2181")
    //消费组 id
    properties.setProperty("group.id", "DataStreamingKafka")

    //flink整合kafka
    //创建一个新的Kafka流媒体源消费者。
    val kafkaSource: FlinkKafkaConsumer09[String] = new FlinkKafkaConsumer09[String](
      //topic
      "DataStreamingKafka",
      //SimpleStringSchema 是序列化和反序列化对信息
      new SimpleStringSchema(),
      properties)

    //添加source
    val kafkaDS: DataStream[String] = environment.addSource(kafkaSource)

    //数据转换：字符串，转成结构化数据(Bean)
    val valueDS: DataStream[ProcessedData] = kafkaDS.map(line => {

      val values: Array[String] = line.split("#CS#")
      val length: Int = values.length
      val regionalRequest: String = if (length > 1) values(1) else ""
      val requestMethod: String = if (length > 2) values(2) else ""
      val contentType: String = if (length > 3) values(3) else ""
      //Post提交的数据体
      val requestBody = if (length > 4) values(4) else ""
      //http_referrer
      val httpReferrer = if (length > 5) values(5) else ""
      //客户端IP
      val remoteAddr = if (length > 6) values(6) else ""
      //客户端UA
      val httpUserAgent = if (length > 7) values(7) else ""
      //服务器时间的ISO8610格式
      val timeIso8601 = if (length > 8) values(8) else ""
      //服务器地址
      val serverAddr = if (length > 9) values(9) else ""
      //获取原始信息中的cookie字符串
      val cookiesStr = if (length > 10) values(10) else ""

      ProcessedData(
        regionalRequest,
        requestMethod,
        contentType,
        requestBody,
        httpReferrer,
        remoteAddr,
        httpUserAgent,
        timeIso8601,
        serverAddr,
        cookiesStr
      )
    })
   // valueDS.print()

    //remoteAddr 远程地址
    val remoteAddr: DataStream[String] = valueDS.map(line => line.remoteAddr)
    val properties01: Properties = new Properties()

    //broker地址
    properties01.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")

    //flink整合kafka，生产者  将数据写入DataStreamingKafka这个topic中
    val kafkaProducer: FlinkKafkaProducer09[String] = new FlinkKafkaProducer09[String](
      //Topic
      "DataStreamingKafka",
      //
      new SimpleStringSchema,
      properties01)
    //发送数据到kafka
    remoteAddr.addSink(kafkaProducer)

    //5.触发执行
    environment.execute()
  }
}
