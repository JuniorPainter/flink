package cn.wi.flink.splitandselect

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingSplitAndSelect
 * @Author: xianlawei
 * @Description: split 将一个数据集切分成两个数据集   通过select查询
 * @date: 2019/9/3 12:17
 */
object DataStreamingSplitAndSelect {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDSInt: DataStream[Int] = environment.fromElements(1, 2, 3, 4, 5)

    dataDSInt.split((line: Int) =>
      line % 2 match {
        case 0 => List("even")
        case 1 => List("odd")
      }).select("even").print()

    //3.数据切分
    //    val splitData: SplitStream[Int] = dataDSInt.split(line => {
    //      line % 2 match {
    //        case 0 => List("even")
    //        case 1 => List("odd")
    //      }
    //    })
    //4.切分流数据查询
    //splitData.select("even").print()

    environment.execute()
  }
}
