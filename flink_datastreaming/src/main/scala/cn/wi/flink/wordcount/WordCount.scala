package cn.wi.flink.wordcount

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: WordCount
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/2 16:32
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //TODO  获取流处理的执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //TODO 数据加载
    val dataDS: DataStream[String] = environment.socketTextStream("localhost", 9999)

    dataDS
      .flatMap((line: String) => line.toLowerCase().split("\\W+"))
      .filter((line: String) => line.nonEmpty)
      .map((line: String) => (line, 1))

      //流处理中数据分组是：KeyBy
      .keyBy(0)
      //数据聚合
      .sum(1)
      //数据打印
      .print()

    environment.execute("start streaming window wordCount")
  }
}
