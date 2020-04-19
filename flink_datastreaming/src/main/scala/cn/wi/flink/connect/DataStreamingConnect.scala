package cn.wi.flink.connect

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataStreamingConnect
 * @Author: xianlawei
 * @Description: 将两个流数据连接在一起
 * @date: 2019/9/2 21:41
 */
object DataStreamingConnect {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val dataDSInt: DataStream[Int] = environment.fromElements(1, 2, 3, 4, 5)
    val dataDSString: DataStream[String] = dataDSInt.map(line => line + "=============")

    //connect操作
    // CoMapFunction<IN1, IN2, OUT>  前两个表示输入参数  后面表示输出参数
    //IN1, IN2 表示两个输入流
    //Int, String, String  前两个表示输入的DS集的数据类型   后面表示输出的数据类型
    dataDSInt.connect(dataDSString)
      //将连接的输入流  进行数据转换
      .map(new CoMapFunction[Int, String, String] {
        //value是第一个数据集  对应IN1  map1函数中编写对IN1输入流的转换操作
        override def map1(value: Int): String = {
          //将Int型转换成String类型   将dataDSInt中的每个元素拼接上====XXXX======
          value + "====XXXX======"
        }

        //value是第二个数据集  对应IN2 map2函数中编写对IN1输入流的转换操作
        override def map2(value: String): String = {
          //将dataDSString中的元素每个值不做操作直接输出
          value
        }
      }).print()

    environment.execute()
  }
}
