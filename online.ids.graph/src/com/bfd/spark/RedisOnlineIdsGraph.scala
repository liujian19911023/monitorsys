package com.bfd.spark;
//package com.bfd.spark
//
//import java.io.FileInputStream
//import java.math.BigDecimal
//import java.text.ParseException
//import java.text.SimpleDateFormat
//import java.util.ArrayList
//import java.util.Date
//import java.util.{ List => JList }
//import java.util.Properties
//import java.util.Timer
//import java.util.TimerTask
//import java.util.concurrent.Executors
//
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.HashMap
//
//import org.apache.curator.framework.CuratorFramework
//import org.apache.curator.framework.CuratorFrameworkFactory
//import org.apache.curator.framework.recipes.cache.NodeCache
//import org.apache.curator.framework.recipes.cache.NodeCacheListener
//import org.apache.curator.retry.RetryNTimes
//import org.apache.spark.SparkConf
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.streaming.Seconds
//import org.apache.spark.streaming.StreamingContext
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.json.JSONArray
//import org.json.JSONException
//import org.json.JSONObject
//
//import com.bfd.proto.GraphProto.Entity
//import com.bfd.proto.GraphProto.EntityRelationInfo
//import com.bfd.proto.GraphProto.Relation
//import com.bfd.spark.model.Id
//import com.bfd.spark.model.IdConf
//import com.bfd.spark.model.RawIds
//import com.bfd.spark.model.TimeFormat
//import com.bfd.spark.schedule.TimeSchedule
//import com.google.protobuf.InvalidProtocolBufferException
//
//import kafka.serializer.StringDecoder
//import redis.clients.jedis.JedisPoolConfig
//import java.util.Calendar
//import java.util.concurrent.TimeUnit
//import redis.clients.jedis.Jedis
//import org.apache.spark.AccumulatorParam
//import org.apache.log4j.Logger
//import java.io.StringWriter
//import java.io.PrintWriter
//import com.bfd.pool.BfdRedisPool
//
////只支持天的topic，暂时不支持多topic
//object RedisOnlineIdsGraph {
//  private var client: CuratorFramework = null
//  private var ssc: StreamingContext = null
//  private var timer: Timer = null
//  private var schedule: TimeSchedule = null
//  private var thread: Thread = null
//  val LOG = Logger.getLogger(RedisOnlineIdsGraph.getClass);
//  def main(args: Array[String]) {
//    if (args.length != 1) {
//      println("Usage spark-submit   --class com.bfd.spark.OnlineIdsGraph  --master yarn  --name onlinegraph  --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=200000  --jars bfdjodis-0.1.2-jar-with-dependencies.jar  --num-executors 10  --driver-memory 10g  --executor-memory 9g  --executor-cores 20 online.ids.graph-0.0.1-SNAPSHOT-jar-with-dependencies.jar online.properties")
//      System.exit(1)
//    }
//    val prop = new Properties()
//    val in = new FileInputStream(args(0))
//    prop.load(in);
//    in.close()
//    val conf_zk_address = prop.getProperty("conf.zk.address")
//    val conf_zk_path = prop.getProperty("conf.zk.path")
//    val kafka_group = prop.getProperty("kafka.group")
//    val kafka_zk_address = prop.getProperty("kafka.zk.address")
//    val key_timeout = prop.getProperty("redis.key.timeout");
//    val redis_key_timeout = Integer.valueOf(key_timeout);
//    val redis_address = prop.getProperty("redis.address")
//    val redis_zk_path = prop.getProperty("redis.zk.path");
//    val bussiness_id = prop.getProperty("businessid");
//    val iterval_second = prop.getProperty("interval.second").toInt
//    val num_of_thread = prop.getProperty("num.kafka.read.thread").toInt
//    val hasprefix = prop.getProperty("kafka.data.hasprefix").toBoolean
//    val hour = prop.getProperty("topic.switch.hour").toInt
//    val minute = prop.getProperty("topic.switch.minute").toInt
//    //获取每天的topic
//    def getTopic() = {
//      val sdf = new SimpleDateFormat("yyyy-MM-dd");
//      sdf.format(new Date());
//    }
//    
//    def yesterday = {
//      val startDT = Calendar.getInstance();
//      startDT.setTime(new Date());
//      startDT.add(Calendar.DAY_OF_MONTH, -1)
//      val yes = startDT.getTime();
//      val sdf = new SimpleDateFormat("yyyy-MM-dd");
//      sdf.format(yes)
//    }
//    
////    def time = {
////      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
////      sdf.format(new Date())
////    }
//
//    var changed = true
//    //切换topic前，需停止启动的sparkstreamcontext以及与zk的连接。
//    def stopLastDay() {
//      val yes = yesterday
//      def extracted() = {
//        while (changed) {
//          LOG.info("topic " + yes + " still has data")
//          TimeUnit.SECONDS.sleep(iterval_second)
//        }
//        LOG.info("determin once again")
//        var need_wait = false
//        for (i <- 0 to 3) {
//          if (changed && !need_wait) need_wait = true
//          TimeUnit.SECONDS.sleep(iterval_second)
//          LOG.info("wait")
//        }
//        need_wait
//      }
//      var need_loop = extracted()
//      while (need_loop) {
//        LOG.info("loop")
//        need_loop = extracted()
//      }
//      if (thread != null) {
//        LOG.info("stop thread")
//        thread.interrupt()
//        thread = null
//      }
//      if (ssc != null) {
//        LOG.info("stop ssc")
//        ssc.stop(true, true)
//        ssc = null
//      }
//      if (client != null) {
//        LOG.info("stop zk client")
//        client.close()
//        client = null
//      }
//      if (timer != null) {
//        LOG.info("stop timer" )
//        timer.cancel()
//        timer = null
//      }
//      if (schedule != null) {
//        LOG.info("stop schedule")
//        schedule = null
//      }
//    }
//    //启动一个spark streaming实例
//    def startStreamContext() = {
//      LOG.info("start ssc")
//      changed = true
//      val sparkConf = new SparkConf()
//      client = CuratorFrameworkFactory
//        .builder()
//        .connectString(conf_zk_address)
//        .retryPolicy(new RetryNTimes(2000, 20000)).build()
//      client.start()
//      ssc = new StreamingContext(sparkConf, Seconds(iterval_second))
//      val buffer = client.getData().forPath(conf_zk_path)
//      var data = new String(buffer)
//      val conf = parseConf(data)
//      LOG.info("init conf " + conf._1)
//      LOG.info("init conf " + conf._2)
//      //将配置广播到没一台服务器
//      var time_format = ssc.sparkContext.broadcast(conf._1)
//      var id_conf = ssc.sparkContext.broadcast(conf._2)
//      //监听配置的变化
//      val pool = Executors.newFixedThreadPool(2)
//      val nodeCache = new NodeCache(client, conf_zk_path, false)
//      nodeCache.start(true)
//      nodeCache.getListenable.addListener(new NodeCacheListener {
//        def nodeChanged() = {
//          val updatedata = new String(nodeCache.getCurrentData().getData())
//          val updateConf = parseConf(updatedata)
//          LOG.info("before update config " + data)
//          LOG.info("after update config " + updatedata)
//          data = updatedata
//          //配置变化先清除以前的广播，并重新广播
//          time_format.unpersist()
//          id_conf.unpersist()
//          time_format = ssc.sparkContext.broadcast(updateConf._1)
//          id_conf = ssc.sparkContext.broadcast(updateConf._2)
//        }
//      }, pool)
//      //根据配置抽取Id,最重要的方法
//      import org.apache.spark.SparkContext.LongAccumulatorParam
//      val accu = ssc.sparkContext.accumulator(0l)
//      var pre = 0l
//      def extract(topicData: String) = {
//        accu += 1
//        var hasException = false
//        var json: JSONObject = null
//        var data = topicData
//        if (hasprefix) {
//          data = topicData.substring(topicData.indexOf("}") + 1)
//        }
//        var id_mapping: HashMap[String, IdConf] = null
//        var method = ""
//        try {
//          json = new JSONObject(data)
//          method = json.getString("method")
//          id_mapping = if (id_conf.value.get(method).isEmpty) null else id_conf.value.get(method).get
//        } catch {
//          case e1: JSONException => {
//            LOG.error(exceptionToString(e1))
//            hasException = true
//          }
//        }
//        if (hasException || null == id_mapping) {
//          (None, false)
//        } else {
//          var ids: RawIds = null
//          try {
//            val method_time_format = time_format.value.get(method).get
//            val timeField = method_time_format.timeField
//            if (!method_time_format.hasField) {
//              ids = new RawIds(System.currentTimeMillis() / 1000)
//            } else {
//              val format = method_time_format.format
//              var timeStr = json.getString(timeField)
//              import java.lang.Double
//              import java.math.BigDecimal
//              val b = new BigDecimal(timeStr);
//              timeStr = b.toPlainString()
//              if (format.equals("timestamp_s")) {
//                ids = new RawIds(Double.valueOf(timeStr).toLong)
//              } else if (format.equals("timestamp_ms")) {
//                ids = new RawIds(Double.valueOf(timeStr).toLong / 1000)
//              } else {
//                ids = new RawIds(string2timeStamp(timeStr, format))
//              }
//            }
//          } catch {
//            case e: JSONException => {
//              LOG.error(exceptionToString(e))
//            }
//          }
//          if (null != ids) {
//            id_mapping.foreach(idmapping => {
//              try {
//                val id_value = json.getString(idmapping._1)
//                val conf = idmapping._2
//                val channel_correlation = conf.channel_correlation
//                var channel = conf.channel
//                if (channel_correlation) {
//                  channel = json.getString(channel)
//                }
//                val id = new Id(channel, conf.id_type, id_value)
//                ids.addId(id)
//              } catch {
//                case e: JSONException => {
//                  LOG.error(exceptionToString(e))
//                }
//              }
//            })
//            if (ids.size() > 0) {
//              (Some(ids), true)
//            } else {
//              (None, false)
//            }
//          } else {
//            (None, false)
//          }
//
//        }
//      }
//      thread = new Thread() {
//        override def run() {
//          while (!Thread.interrupted()) {
//            if (accu.value - pre == 0) changed = false else changed = true
//            pre = accu.value
//            LOG.info("monitor thread accu size is " + pre)
//            TimeUnit.SECONDS.sleep(iterval_second)
//          }
//        }
//      }
//      thread.start()
//
//      import org.apache.spark.streaming.StreamingContext._
//      //建立与kafka之间的连接，并启动ssc
//      val kafkaParams: Map[String, String] = Map("group.id" -> kafka_group, "auto.offset.reset" -> "smallest", "zookeeper.connect" -> kafka_zk_address,
//        "zookeeper.session.timeout.ms" -> "10000", "zookeeper.sync.time.ms" -> "200", "auto.commit.interval.ms" -> "1000")
//      val topic = getTopic()
//      val topicMap = Map(topic -> num_of_thread)
//      val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
//
//      val id_relations = lines.map { line => extract(line) }.filter(tuple => tuple._2).map(tuple => tuple._1).flatMap { ids => split(ids.get) }
//
//      id_relations.groupByKey().
//        foreachRDD(rdd => {
//
//          rdd.foreachPartition(partitionOfRecords => {
//
//            BfdRedisPool.init(redis_address)
//            val bfdjodis = BfdRedisPool.getInstance.getResource
//            try {
//              partitionOfRecords.foreach(pair => {
//                //新数据与codis中的旧数据合并，如何旧数据中id的个数超过100，抛弃，否则执行更新和添加操作
//                val redisKey = bussiness_id + ":" + pair._1
//                //bfdjodis = BfdRedisSingleHelper.getInstance().getResource
//                val oldrelation = bfdjodis.get(redisKey)
//                val erinfoBuilder = EntityRelationInfo.newBuilder();
//                val updated = new HashMap[Id, Long]
//                for (ids <- pair._2) {
//                  val time = ids.relation_time
//                  for (id <- ids.getIds()) {
//                    if (updated.contains(id)) {
//                      if (updated.get(id).get < time) {
//                        updated.put(id, time)
//                      }
//                    } else {
//                      updated.put(id, time)
//                    }
//                  }
//                }
//                if (oldrelation != null) {
//                  try {
//                    val oldEntityRelation = EntityRelationInfo.parseFrom(oldrelation.getBytes("ISO-8859-1"));
//                    val relations = oldEntityRelation.getRelationList();
//                    for (id <- updated.keySet) {
//                      val relationid = Entity.newBuilder().setChannel(id.channel).setType(id.id_type).setValue(id.id).build()
//                      val relation = Relation.newBuilder().setEntity(relationid).setUpdateTime(updated.get(id).get).build()
//                      erinfoBuilder.addRelation(relation);
//                    }
//                    val lst = new ArrayList[Relation](relations)
//                    if (relations.size() < 100) {
//                      //历史数据小于阈值合并，超过阈值删除历史
//                      val need_move = new ArrayList[Relation]();
//                      for (id <- updated.keySet) {
//                        val tmp = containRelation(relations, id);
//                        if (null != tmp) {
//                          need_move.add(tmp);
//                        }
//                      }
//                      lst.removeAll(need_move);
//                      val filter_iter = lst.iterator()
//                      while (filter_iter.hasNext()) {
//                        val filter = filter_iter.next()
//                        val relation = Relation.newBuilder().setEntity(filter.getEntity()).setUpdateTime(filter.getUpdateTime()).build()
//                        erinfoBuilder.addRelation(relation);
//                      }
//                    }
//                  } catch {
//                    case e: InvalidProtocolBufferException => { LOG.error(exceptionToString(e)) }
//                  }
//                } else {
//                  for (id <- updated.keySet) {
//                    val relationid = Entity.newBuilder().setChannel(id.channel).setType(id.id_type).setValue(id.id).build()
//                    val relation = Relation.newBuilder().setEntity(relationid).setUpdateTime(updated.get(id).get).build()
//                    erinfoBuilder.addRelation(relation);
//                  }
//                }
//                val erinfo = erinfoBuilder.build()
//                bfdjodis.set(redisKey, new String(erinfo.toByteArray(), "ISO-8859-1"))
//                bfdjodis.expire(redisKey, redis_key_timeout)
//              })
//            } finally {
//              BfdRedisPool.getInstance.returnResourceObject(bfdjodis)
//            }
//
//          })
//        })
//      ssc.start()
//      ssc.awaitTermination()
//    }
//
//    //切换topic的定时任务
//
//    class SwitchTask extends TimerTask {
//      override def run() {
//        LOG.info("stop last day")
//        stopLastDay()
//        LOG.info("register task")
//        registerTask()
//        LOG.info("start ssc")
//        startStreamContext()
//      }
//    }
//    def registerTask() {
//      timer = new Timer()
//      schedule = new TimeSchedule(timer)
//      schedule.addFixedTask(hour, minute, 0, new SwitchTask)
//    }
//    registerTask
//    startStreamContext()
//  }
//  //判断历史是否包含新进的Id
//  def containRelation(relations: JList[Relation], id: Id): Relation = {
//    val iter = relations.iterator()
//    while (iter.hasNext()) {
//      val rela = iter.next()
//      val old = rela.getEntity();
//      if (id.channel.equals(old.getChannel()) && id.id_type.equals(old.getType()) && id.id.equals(old.getValue())) {
//        return rela
//      }
//    }
//    return null
//
//  }
//  //将Id拆分，防止同一个Id在不同机器上写codis，造成数据覆盖，导致数据不一致。
//  def split(ids: RawIds) = {
//    val res = ArrayBuffer[(String, RawIds)]()
//    for (id <- ids.idSet) {
//      res += ((id.toString(), ids))
//    }
//    res
//  }
//  //根据时间格式将时间统一转为时间戳
//  def string2timeStamp(date: String, format: String) = {
//    try {
//      val sdf = new SimpleDateFormat(format);
//      val dat = sdf.parse(date);
//      dat.getTime() / 1000;
//    } catch {
//      case e: ParseException => {
//        LOG.error(exceptionToString(e))
//        System.currentTimeMillis() / 1000
//      }
//    }
//  }
//  //解析zookeeper中的配置
//  def parseConf(data: String) = {
//    val time_format = new HashMap[String, TimeFormat]()
//    val id_conf = new HashMap[String, HashMap[String, IdConf]]()
//    val json = new JSONObject(data)
//    val topic_type = json.getBoolean("switchFlag")
//    val methodsArray = new JSONArray(json.getString("methods"))
//    for (i <- 0 until methodsArray.length()) {
//      val methodobj = methodsArray.getJSONObject(i)
//      val method = methodobj.getString("method")
//      var timeField = ""
//      val hastimeField = methodobj.has("timeField")
//      val format = methodobj.getString("timeStyle")
//      if (hastimeField) {
//        timeField = methodobj.getString("timeField")
//      }
//      time_format.put(method, new TimeFormat(format, hastimeField, timeField))
//      val idsArray = methodobj.getJSONArray("ids")
//      val methodMap = new HashMap[String, IdConf]
//      id_conf.put(method, methodMap)
//      parseIdsConf(methodMap, idsArray)
//    }
//    (time_format, id_conf)
//  }
//  //解析Id的配置
//  def parseIdsConf(id_conf: HashMap[String, IdConf], idsArray: JSONArray) {
//    for (i <- 0 until idsArray.length()) {
//      val id = idsArray.getJSONObject(i)
//      val channel = id.getString("channel")
//      val id_type = id.getString("type")
//      val idField = id.getString("id")
//      val dynamic = id.getString("dynamic")
//      import java.lang.Boolean
//      id_conf.put(idField, new IdConf(channel, id_type, Boolean.valueOf(dynamic)))
//    }
//  }
//
//  def exceptionToString(e: Throwable) = {
//    val sw = new StringWriter()
//    val pw = new PrintWriter(sw, true)
//    e.printStackTrace(pw);
//    pw.flush()
//    sw.flush()
//    sw.toString();
//  }
//
//}