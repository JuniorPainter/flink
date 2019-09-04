package cn.wi.flink.source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: DataSetHDFS
 * @Author: xianlawei
 * @Description: 读取HDFS上面的文件
 * @date: 2019/9/2 8:46
 */
object DataSetHDFS {
  def main(args: Array[String]): Unit = {
    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val dataDS: DataSet[String] = environment.readTextFile("hdfs://node01:8020/flink/input/wordcount.data")
    //3. 指定数据转换
    val wordDS: DataSet[(String, Int)] = dataDS.flatMap(line =>
      line
        .split("\\s+")
        .filter(line => line.nonEmpty)
        .map(word => (word, 1))
    )
    //二元组 (word, 1)  按照K进行分组  按照V进行求和
    wordDS
      .groupBy(0)
      .sum(1)
      .print()
  }
}
