package cn.wi.flink

/**
 * @ProjectName: Flink_Parent
 * @Author: xianlawei
 * @Description:
 * @Date: 2020/4/21
 */
//4)创建一个订单样例类 Order ，包含四个字段（订单ID、用户ID、订单金额、时间戳）
case class Order(orderId:String,userId:Int,orderPrice:Double,orderTime:Long)
