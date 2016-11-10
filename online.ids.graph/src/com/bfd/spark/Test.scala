package com.bfd.spark

import com.bfd.pool.BfdRedisPool

object Test extends App{
  val t1=System.currentTimeMillis()
  BfdRedisPool.init("172.24.5.63:6379")
  val redis=BfdRedisPool.getInstance().getResource
  val t2=System.currentTimeMillis()
  for(i<- 0 to 200){
    redis.sadd("all:b:global:gid:12345678","b:global:gid:"+i)
  }
  val t3=System.currentTimeMillis()
  redis.sadd("all:b:global:gid:12345678", "b:global:gid:12345678")
  import scala.collection.JavaConversions._
  val all_id=redis.smembers("all:b:global:gid:12345678")
  for(s<-all_id){
    val st=redis.smembers("all:"+s)
    if(!st.isEmpty()){
      println(st)
    }
  }
  val t4=System.currentTimeMillis()
  println(t4-t1)
  println(t3-t1)
  println(t2-t1)
  BfdRedisPool.getInstance().returnResourceObject(redis)
  
}