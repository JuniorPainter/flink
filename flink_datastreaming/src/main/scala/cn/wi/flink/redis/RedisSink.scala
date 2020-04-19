package cn.wi.flink.redis

import java.net.{InetAddress, InetSocketAddress}
import java.util
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 将数据写入Redis
 * @Date: 2020/4/19
 */
object RedisSink {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[String] = environment.socketTextStream("node01", 8090)

    //3.将数据求和
    val result: DataStream[(String, Int)] = source.flatMap(_.split("\\W+"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    //将结果放入Redis
    val inetSocketAddress: util.HashSet[InetSocketAddress] = new util.HashSet[InetSocketAddress]()
    inetSocketAddress.add(new InetSocketAddress(InetAddress.getByName("node01"), 7001))
    inetSocketAddress.add(new InetSocketAddress(InetAddress.getByName("node01"), 7002))
    inetSocketAddress.add(new InetSocketAddress(InetAddress.getByName("node01"), 7003))

    //配置对象
    val flinkJedisClusterConfig: FlinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
      .setNodes(inetSocketAddress)
      //池分配的最大对象数
      .setMaxTotal(5)
      .build()

    result.addSink(new RedisSink(flinkJedisClusterConfig,new MyRedisSink))

    environment.execute()
  }
}
