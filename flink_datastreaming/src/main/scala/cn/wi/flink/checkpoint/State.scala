package cn.wi.flink.checkpoint

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: State
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 14:43
 */
class State extends Serializable {
  var total: Long = 0L

  def getTotal = total

  def setTotal(value: Long) = {
    total = value
  }
}
