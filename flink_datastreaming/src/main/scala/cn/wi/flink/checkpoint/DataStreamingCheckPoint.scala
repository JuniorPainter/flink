package cn.wi.flink.checkpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingCheckPoint
 * @Author: xianlawei
 * @Description:
 * 设置检查点--6s触发一次
 * 自定义数据源：每秒钟产生大约10000条数据
 * 四元组(Long，String，String，Integer)—------(id,name,info,count)
 * 数据转换
 * 每隔1秒钟统计一次最近4秒
 * 统计结果
 * 打印数据
 * @date: 2019/9/3 14:28
 */
object DataStreamingCheckPoint {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    environment.setParallelism(1)

    environment.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink/checkpoint"))

    //没隔6s触发一次
    environment.enableCheckpointing(6000)
    //强一致性
    //检查点模式：CheckpointingMode.EXACTLY_ONCE  强一致性  如果当前算子出现问题 会强制将数据量恢复到上一个算子的位置
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    //检查点之间的间隔时间   每个算子每隔多长时间制作一次检查点
    environment.getCheckpointConfig.setCheckpointInterval(6000)
    //检查点超时时间
    environment.getCheckpointConfig.setCheckpointTimeout(60000)

    //检查点制作失败 程序会报错  默认是true
    environment.getCheckpointConfig.setFailOnCheckpointingErrors(false)

    //当检查点制作失败的时候，task继续运行
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //RETAIN_ON_CANCELLATION:取消任务的时候，保留检查点，需要手动删除 (生产环境使用)
    ///DELETE_ON_CANCELLATION:取消任务的时候,自动删除检查点，不需要手动删除

    environment.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig
        .ExternalizedCheckpointCleanup
        .RETAIN_ON_CANCELLATION
    )

    //自定义数据源
    environment
      .addSource(new CheckpointSource)
      .keyBy(line => line.id)
      // 每隔1秒钟统计一次最近4秒
      .timeWindow(Time.seconds(4), Time.seconds(1))
      //复杂业务用apply
      .apply(new CheckpointFunction)
      .print()

    environment.execute()
  }
}
