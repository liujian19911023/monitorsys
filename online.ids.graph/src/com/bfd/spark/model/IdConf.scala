package com.bfd.spark.model

import java.io.Serializable
/**
 * 代表zk中一个id的配置
 */
class IdConf(val channel: String, val id_type: String, val channel_correlation: Boolean) extends Serializable {

}