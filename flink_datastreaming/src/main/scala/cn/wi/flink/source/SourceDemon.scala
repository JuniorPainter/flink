package cn.wi.flink.source

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 并行度数据源
 * @Date: 2020/4/19
 */
object SourceDemon {
  def main(args: Array[String]): Unit = {
    //获取执行环境
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //自定义单数据源
    //environment.addSource(new SingleParallelismCustomizeSource).print()

    //自定义多并行度数据源
    environment.addSource(new MultiParallelismCustomizeSource).setParallelism(1).print()


    environment.execute()
  }
}
