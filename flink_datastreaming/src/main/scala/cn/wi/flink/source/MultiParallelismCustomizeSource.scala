package cn.wi.flink.source

import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 多并行度
 * @Date: 2020/4/19
 */
//RichParallelSourceFunction[Int]  [Int]的输出类型
class MultiParallelismCustomizeSource extends RichParallelSourceFunction[Int] {
  override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
    var i:Int = 0
    while (true){
      i+=1
      ctx.collect(i)
      Thread.sleep(1000)
    }
  }


  override def setRuntimeContext(t: RuntimeContext): Unit = super.setRuntimeContext(t)

  override def getRuntimeContext: RuntimeContext = super.getRuntimeContext

  override def getIterationRuntimeContext: IterationRuntimeContext = super.getIterationRuntimeContext

  override def cancel(): Unit = ???
}
