package com.bfd.spark.model

import java.io.Serializable
/**
 * 根据配置解析后的一个id，channel、id_type和id三个维度唯一代表一个id
 */
class Id(val channel: String, val id_type: String, val id: String) extends Serializable {

  override def toString() = {
    channel + ":" + id_type + ":" + id

  }

  def canEqual(a: Any) = a.isInstanceOf[Id]
  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (channel == null) 0 else channel.hashCode())
    result = prime * result + (if (id == null) 0 else id.hashCode())
    result = prime * result + (if (id_type == null) 0 else id_type.hashCode())
    return result;
  }
  override def equals(that: Any): Boolean =
    that match {
      case that: Id => that.canEqual(this) && this.hashCode == that.hashCode
      case _        => false
    }

}