package cn.wi.flink.mysql

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/19
 */
object MySQLSource {
  def main(args: Array[String]): Unit = {

    //1.获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.自定义数据源，读取mysql数据
    env.addSource(new MySqlSourceDemo)
      .print()

    //3.触发执行
    env.execute()
  }
}
