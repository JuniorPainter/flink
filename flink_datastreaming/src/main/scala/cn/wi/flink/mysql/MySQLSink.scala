package cn.wi.flink.mysql

import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 将数据写入MySQL
 * @Date: 2020/4/19
 */
object MySQLSink {
  def main(args: Array[String]): Unit = {
    //获取流处理执行环境
    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //加载数据源
    val source: DataStreamSource[Demon] = executionEnvironment.fromElements(Demon(20, "xiao", 20))

    //数据写入
    source.addSink(new SinkMySQL)
    executionEnvironment.execute()
  }
}
