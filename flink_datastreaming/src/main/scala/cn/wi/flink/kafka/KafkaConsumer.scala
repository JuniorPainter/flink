package cn.wi.flink.kafka

import java.{lang, util}
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
object KafkaConsumer {
  def main(args: Array[String]): Unit = {
    /**
     * 1.获取流处理执行环境
     * 2.配置kafka参数
     * 3.整合kafka
     * 4.设置kafka消费者模式
     * 5.加载数据源
     * 6.数据打印
     * 7.触发执行
     */

    //获取流处理执行环境
    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //配置Kafka参数
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    properties.setProperty("group.id", "test1017")
    //最近消费，与offset相关，从消费组上次消费的偏移量开始消费 kafkaConsumer.setStartFromGroupOffsets()
    properties.setProperty("auto.offset.reset", "latest")

    //整合Kafka  FlinkKafkaProducer09  后面的编号是Kafka依赖的版本决定的  选择向前兼容的一个版本
    val kafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String]("demo", new SimpleStringSchema(), properties)

    //设置Kafka消费模式
    //默认值，当前消费组记录的偏移量开始，接着上次的偏移量消费
    //kafkaConsumer.setStartFromGroupOffsets()
    //从头消费
    //kafkaConsumer.setStartFromEarliest()
    //从最近消费，与Offset无关，会导致数据丢失
    //kafkaConsumer.setStartFromLatest()

    //指定偏移量消费数据
    // val map = new util.HashMap[KafkaTopicPartition, lang.Long]()
    //6L  偏移量
    //    map.put(new KafkaTopicPartition("demo", 0), 6L)
    //    map.put(new KafkaTopicPartition("demo", 1), 6L)
    //    map.put(new KafkaTopicPartition("demo", 2), 6L)
    //
    //    kafkaConsumer.setStartFromSpecificOffsets(map)

    val consumer: DataStream[String] = executionEnvironment.addSource(kafkaConsumer)
    consumer.print()
    executionEnvironment.execute()
  }
}
