package com.bfd.spark.model

import scala.collection.mutable.HashSet
import java.io.Serializable
/**
 * 每条记录中抽取出的所有解析后的Id信息,也就是直接关系。
 */
class RawIds(val relation_time: Long, val idSet: HashSet[Id]) extends Serializable {
  def this(relation_time: Long) = { this(relation_time, new HashSet[Id]) }
  def addId(id: Id) {
    idSet.add(id)
  }
  def size() = {
    idSet.size
  }
  def getIds() = {
    idSet
  }
}