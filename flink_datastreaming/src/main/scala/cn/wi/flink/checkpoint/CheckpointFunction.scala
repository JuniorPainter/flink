package cn.wi.flink.checkpoint

import java.util

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: CheckpointFunction
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 14:40
 */
case class CheckpointFunction()
  extends WindowFunction[Info, Long, Long, TimeWindow]
    with ListCheckpointed[State] {
  var total: Long = 0L

  //统计结果
  override def apply(key: Long, window: TimeWindow, input: Iterable[Info], out: Collector[Long]): Unit = {
    var count: Long = 0L
    for (line <- input) {
      count = count + line.count
    }
    total += count
    out.collect(total)
  }

  //制作快照
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[State] = {
    val list: util.ArrayList[State] = new util.ArrayList[State]()
    val state = new State
    state.setTotal(total)
    list.add(state)
    list
  }

  //数据恢复
  override def restoreState(state: util.List[State]): Unit = {
    total = state.get(0).getTotal
  }
}
