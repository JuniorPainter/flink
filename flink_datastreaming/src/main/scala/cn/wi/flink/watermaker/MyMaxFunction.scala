package cn.wi.flink.watermaker

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: MyMaxFunction
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/2 21:26
 */
//WindowFunction[IN输入, OUT输出, KEY 以什么类型进行分组的, W <: Window 窗口类型]
class MyMaxFunction extends WindowFunction[Boss, Double, String, TimeWindow] {
  //复写父类的方法
  override def apply(
                      key: String,
                      window: TimeWindow,
                      input: Iterable[Boss],
                      out: Collector[Double]): Unit = {
    val bosses: Array[Boss] = input
      .toArray
      //默认升序
      .sortBy(line => line.price)
      //反转
      .reverse
      //取最大
      .take(1)

    //对Arrary遍历取值
    for (line <- bosses) {
      out.collect(line.price)
    }
  }
}
