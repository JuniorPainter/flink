package cn.wi.flink.kafka

/**
 * @ProjectName: Flink_Parent 
 * @ClassName: ProcessedData
 * @Author: xianlawei
 * @Description:
 * @date: 2019/9/3 13:47
 */
case class ProcessedData(
                          regionalRequest: String,
                          requestMethod: String,
                          contentType: String,
                          requestBody: String,
                          httpReferrer: String,
                          remoteAddr: String,
                          httpUserAgent: String,
                          timeIso8601: String,
                          serverAddr: String,
                          cookiesStr: String
                        )
