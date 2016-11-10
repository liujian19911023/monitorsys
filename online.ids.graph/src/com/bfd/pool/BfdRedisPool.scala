package com.bfd.pool

import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

object BfdRedisPool {

  @volatile private[this] var inited = false
  @volatile private[this] var instanced = false
  private[this] var jedisPool: JedisPool = _
  private[this] var address: String = _
  def init(address: String) {
    if (!inited) {
      this.synchronized {
        if (!inited) {
          this.address = address
          inited = true
        }
      }
    }
  }
  def getInstance() = {
    if (!instanced) {
      this.synchronized {
        if (!instanced) {
          val ip_port = address.split(":")
          val config = new JedisPoolConfig()
          config.setMaxTotal(100)
          config.setMinIdle(10)
          config.setMaxIdle(10)
          config.setMaxWaitMillis(5000)
          config.setTestWhileIdle(false)
          config.setTestOnBorrow(false)
          config.setTestOnReturn(false)
          jedisPool = new JedisPool(config, ip_port(0), ip_port(1).toInt)
          val thread = new Thread() {
            override def run() = {
              jedisPool.destroy()
              jedisPool = null
            }
          }
          Runtime.getRuntime().addShutdownHook(thread)
          instanced = true
        }

      }
    }
    jedisPool

  }
}