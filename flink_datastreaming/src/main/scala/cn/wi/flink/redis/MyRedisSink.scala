package cn.wi.flink.redis

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
//[(String,Int)] 输入的数据类型
class MyRedisSink extends RedisMapper[(String,Int)]{
  //指定redis的数据类型
  override def getCommandDescription: RedisCommandDescription = {
  new RedisCommandDescription(RedisCommand.HSET,"sinkRedis")
  }

  //redis Key
  override def getKeyFromData(data: (String, Int)): String = {
    data._1
  }

  //redis value
  override def getValueFromData(data: (String, Int)): String = {
    data._2.toString
  }
}
