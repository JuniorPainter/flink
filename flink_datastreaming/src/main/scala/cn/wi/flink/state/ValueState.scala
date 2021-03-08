package cn.wi.flink.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/20
 */
object ValueState {
  def main(args: Array[String]): Unit = {
    /**
     * 开发步骤：
     *       1.获取流处理执行环境
     *       2.加载数据源,以及数据处理
     *       3.数据分组
     *       4.数据转换，定义ValueState,保存中间结果
     *       5.数据打印
     *       6.触发执行
     */

    val executionEnvironment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val source: DataStream[(Long, Long)] = executionEnvironment.fromCollection(
      List(
        (1L, 4L),
        (2L, 3L),
        (3L, 1L),
        (1L, 2L),
        (3L, 2L),
        (1L, 2L),
        (2L, 2L),
        (2L, 9L)))

    val keyData: KeyedStream[(Long, Long), Tuple] = source.keyBy(0)

    //数据转换  定义ValueState，保存中间结果
    keyData.map(
      new RichMapFunction[(Long, Long), (Long, Long)] {
        var valueState: ValueState[(Long, Long)] = _

        //定义ValueState
        override def open(parameters: Configuration) = {
          //新建状态描述器
          val valueStateDescriptor: ValueStateDescriptor[(Long, Long)] = new ValueStateDescriptor[(Long, Long)](
            "valueState",
            TypeInformation.of(
              //泛型
              new TypeHint[(Long, Long)] {}))
          //获取ValueState
          valueState = getRuntimeContext.getState(valueStateDescriptor)
        }

        //计算并保存中间结果
        override def map(value: (Long, Long)) = {
          //获取valueState内的值
          val tuple: (Long, Long) = valueState.value()
          val currentType: (Long, Long) = if (tuple == null) {
            (0L, 0L)
          } else {
            tuple
          }
          val tupleResult: (Long, Long) = (value._1, value._2 + currentType._2)

          //更新valueState
          valueState.update(tupleResult)

          tupleResult
        }
      }
    ).print()

    executionEnvironment.execute()
  }
}
