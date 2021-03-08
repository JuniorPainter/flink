package cn.wi.flink.timer

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/5/6
 */
class MySourceTuple extends SourceFunction[(String, Long)] {
  var isRunning: Boolean = true
  val names: List[String] = List("张", "王", "李", "赵")
  private val random = new Random()
  var number: Long = 1

  override def run(ctx: SourceFunction.SourceContext[(String, Long)]): Unit = {
    while (true) {
      val index: Int = random.nextInt(4)
      ctx.collect((names(index), number))
      number += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
