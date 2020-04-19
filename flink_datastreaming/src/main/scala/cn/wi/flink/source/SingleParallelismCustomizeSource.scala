package cn.wi.flink.source

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description: 自定义数据源  只有单线程
 * @Date: 2020/4/19
 */
//RichSourceFunction<OUT>  [Int]输出的数据类型  但数据源不能使用多个并行度
class SingleParallelismCustomizeSource extends RichSourceFunction[Int]{
  override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
    var i=0

    while (true){
      i+=1
      ctx.collect(i)
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = ???
}
