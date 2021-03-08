package cn.wi.flink

import java.util.UUID

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import scala.util.Random
/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/21
 */
class OrderSourceFunc extends RichSourceFunction[Order] {
  /**
   * * a.使用for循环生成1000个订单
   * * b.随机生成订单ID（UUID）
   * * c.随机生成用户ID（0-2）
   * * d.随机生成订单金额（0-100）
   * * e.时间戳为当前系统时间
   * * f.每隔1秒生成一个订单
   *
   * @param ctx
   */
  override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {

    for(line<- 0 until 1000){
      ctx.collect(Order(UUID.randomUUID().toString,Random.nextInt(2),Random.nextInt(100),System.currentTimeMillis()))
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = ???
}
