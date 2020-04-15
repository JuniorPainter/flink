package cn.wi.flink.wordcount

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: WordCount
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/1 15:42
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1. 获取Flink执行环境
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    //2. 加载/创建初始数据源
    val wordCountDS: DataSet[String] = environment.fromElements("Who's there? I think I hear them. Stand, ho! Who's there?")

    //3. 指定数据转换
    val wordDS: DataSet[(String, Int)] = wordCountDS.flatMap((line: String) =>
      line
        .toLowerCase()
        .split("\\W+")
        .filter((line: String) => line.nonEmpty)
        .map((word: String) => (word, 1))
    )
    //二元组 (word, 1)  按照K进行分组  按照V进行求和
    wordDS
      .groupBy(0)
      .sum(1)
      .writeAsText("D:\\Flink\\output\\WordCount", WriteMode.OVERWRITE)

    //触发程序执行
    environment.execute()
  }
}
