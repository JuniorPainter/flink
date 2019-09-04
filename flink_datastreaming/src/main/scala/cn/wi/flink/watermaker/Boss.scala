package cn.wi.flink.watermaker

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: Boss
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/2 21:26
 */
//数据：(时间，公司，产品，价格)
//1527911155000,boos1,pc1,100.0
case class Boss(timestamp: Long, boss: String, product: String, price: Double)
