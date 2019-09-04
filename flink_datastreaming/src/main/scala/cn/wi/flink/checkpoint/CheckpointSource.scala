package cn.wi.flink.checkpoint

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: CheckpointSource
 * @Author: xianlawei
 * @Description: 每秒钟产生大概1000条数据
 * @date: 2019/9/3 14:40
 */
class CheckpointSource extends RichSourceFunction[Info] {

  //主要业务逻辑执行方法
  override def run(ctx: SourceFunction.SourceContext[Info]): Unit = {
    while (true) {
      for (line <- 0 until 1000) {
        ctx.collect(
          Info(1, "test-" + line, "test-info", 1)
        )
      }
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???
}
