package cn.wi.flink.operator

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/15
 */
object DataSetLeftOuterJoin {
  def main(args: Array[String]): Unit = {
    val executionEnvironment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //2.加载数据源
    val s1: DataSet[(Int, String)] = executionEnvironment.fromElements((1, "zhangsan") , (2, "lisi") ,(3 , "wangwu") ,(4 , "zhaoliu"))
    val s2: DataSet[(Int, String)] = executionEnvironment.fromElements((1, "beijing"), (2, "shanghai"), (4, "guangzhou"))

    //Join操作
    //LeftJoin
    s1.leftOuterJoin(s2).where(0).equalTo(0){
      (s1: (Int, String), s2: (Int, String))=>{
        if (s2==null) {
          (s1._1,s1._2,null)
        }else{
          (s1._1,s1._2,s2._2)
        }
      }
    }.print()
  }
}