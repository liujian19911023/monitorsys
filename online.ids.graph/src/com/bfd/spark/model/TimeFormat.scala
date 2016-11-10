package com.bfd.spark.model

import java.io.Serializable
/**
 * 每个method可以配置一个时间戳来源，包括字段，格式，等等，此model代表从zk中解析的关于时间的配置信息
 */
class TimeFormat(val format: String, val hasField: Boolean, val timeField: String) extends Serializable {

}