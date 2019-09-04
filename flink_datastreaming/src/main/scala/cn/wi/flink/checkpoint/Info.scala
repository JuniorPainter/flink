package cn.wi.flink.checkpoint

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: Info
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 14:41
 */
//四元组(Long，String，String，Integer)—------(id,name,info,count)
case class Info(id: Long, name: String, info: String, count: Int)
