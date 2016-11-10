package com.bfd.spark

import java.io.FileInputStream
import java.io.PrintWriter
import java.io.StringWriter
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.ArrayList
import java.util.Calendar
import java.util.Date
import java.util.{ List => JList }
import java.util.Properties
import java.util.Timer
import java.util.TimerTask
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.util.control._
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.cache.NodeCache
import org.apache.curator.framework.recipes.cache.NodeCacheListener
import org.apache.curator.retry.RetryNTimes
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json.JSONArray
import org.json.JSONException
import org.json.JSONObject
import com.bfd.spark.model.Id
import com.bfd.spark.model.IdConf
import com.bfd.spark.model.RawIds
import com.bfd.spark.model.TimeFormat
import com.bfd.spark.schedule.TimeSchedule
import kafka.producer.KeyedMessage
import kafka.serializer.StringDecoder
import org.apache.log4j.PropertyConfigurator
import com.bfd.pool.BfdRedisPool
import com.bfd.pool.KafkaProducerPool
import java.math.BigDecimal

/**
 * monitor system
 * author jian.liu
 */
object OnlineMonitorSystem {
  private var client: CuratorFramework = null
  private var ssc: StreamingContext = null
  private var timer: Timer = null
  private var schedule: TimeSchedule = null
  private var thread: Thread = null
  private var conf_node: NodeCache = null
  private var pool: ExecutorService = null
  val LOG = Logger.getLogger(OnlineMonitorSystem.getClass);
  def main(args: Array[String]) {
    if (args.length != 2) {
      println("Usage spark-submit   --class com.bfd.spark.OnlineIdsGraph  --master yarn  --name onlinegraph  --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.receiver.maxRate=200000  --jars bfdjodis-0.1.2-jar-with-dependencies.jar  --num-executors 10  --driver-memory 10g  --executor-memory 9g  --executor-cores 20 online.ids.graph-0.0.1-SNAPSHOT-jar-with-dependencies.jar online.properties")

    }
    val prop = new Properties()
    val in = new FileInputStream(args(0))
    prop.load(in);
    in.close()
    PropertyConfigurator.configure(args(1));
    val conf_zk_address = prop.getProperty("conf.zk.address")
    val conf_zk_path = prop.getProperty("conf.zk.path")
    val kafka_group = prop.getProperty("kafka.group")
    val kafka_zk_address = prop.getProperty("kafka.zk.address")
    val redis_address = prop.getProperty("redis.address")
    val redis_zk_path = prop.getProperty("redis.zk.path");
    val set_redis_timeout = prop.getProperty("need.redis.timeout").toBoolean
    println(set_redis_timeout)
    println(prop.contains("redis.key.timeout"))
    if (set_redis_timeout) {
      if (!prop.containsKey("redis.key.timeout")) {
        println("you need set redis timeout,but not contain redis.key.timeout configuration")
        System.exit(1)
      }
    }
    val time_out = if (set_redis_timeout) prop.getProperty("redis.key.timeout").toInt else 0;
    val bussiness_id = prop.getProperty("monitor.businessid");
    val iterval_second = prop.getProperty("interval.second").toInt
    val num_of_thread = prop.getProperty("num.kafka.read.thread").toInt
    val hasprefix = prop.getProperty("kafka.data.hasprefix").toBoolean
    val hour = prop.getProperty("topic.switch.hour").toInt
    val minute = prop.getProperty("topic.switch.minute").toInt
    val monitor_topic = prop.getProperty("monitor.topic")
    val metadata_broker_list = prop.getProperty("meta.broker.list")
    val request_required_acks = prop.getProperty("kafka.required.acks")
    val producer_type = prop.getProperty("producer.type")

    //get daily topic
    def getTopic() = {
      val sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.format(new Date());
    }

    def yesterday = {
      val startDT = Calendar.getInstance();
      startDT.setTime(new Date());
      startDT.add(Calendar.DAY_OF_MONTH, -1)
      val yes = startDT.getTime();
      val sdf = new SimpleDateFormat("yyyy-MM-dd");
      sdf.format(yes)
    }

    var changed = true
    //before switch to new topic,release resource
    def stopLastDay() {
      val yes = yesterday
      def extracted() = {
        while (changed) {
          LOG.info("topic " + yes + " still has data")
          TimeUnit.SECONDS.sleep(iterval_second)
        }
        LOG.info("determine once again")
        var need_wait = false
        for (i <- 0 to 3) {
          if (changed && !need_wait) need_wait = true
          TimeUnit.SECONDS.sleep(iterval_second)
          LOG.info("wait")
        }
        need_wait
      }
      var need_loop = extracted()
      while (need_loop) {
        LOG.info("loop")
        need_loop = extracted()
      }
      if (thread != null) {
        LOG.info("stop thread")
        thread.interrupt()
        thread = null
      }
      if (ssc != null) {
        LOG.info("stop ssc")
        ssc.stop(true, true)
        ssc = null
      }
      if (client != null) {
        LOG.info("stop zk client")
        client.close()
        client = null
      }
      if (timer != null) {
        LOG.info("stop timer")
        timer.cancel()
        timer = null
      }
      if (schedule != null) {
        LOG.info("stop schedule")
        schedule = null
      }
      if (conf_node != null) {
        LOG.info("close conf watcher")
        conf_node.close()
      }
      if (pool != null) {
        LOG.info("shutdown watcher executor pool")
        pool.shutdown()
      }
    }
    def genData(id: String, n_id: scala.collection.Set[_ <: String], data: String) = {
      val json = new JSONObject();
      json.put("id", id)
      json.put("n_id", n_id.mkString(","))
      json.put("data", data)
      json.toString()
    }

    //start a spark streaming job
    def startStreamContext() = {
      import org.apache.spark.SparkContext.LongAccumulatorParam
      import org.apache.spark.streaming.StreamingContext._
      LOG.info("start ssc")
      changed = true
      val sparkConf = new SparkConf()
      client = CuratorFrameworkFactory
        .builder()
        .connectString(conf_zk_address)
        .retryPolicy(new RetryNTimes(2000, 20000)).build()
      client.start()
      ssc = new StreamingContext(sparkConf, Seconds(iterval_second))
      val buffer = client.getData().forPath(conf_zk_path)
      var data = new String(buffer)
      val conf = parseConf(data)
      LOG.info("init conf " + conf._1)
      LOG.info("init conf " + conf._2)
      //broadcast configuration 
      var time_format = ssc.sparkContext.broadcast(conf._1)
      var id_conf = ssc.sparkContext.broadcast(conf._2)
      //watch configuration  change
      pool = Executors.newFixedThreadPool(2)
      conf_node = new NodeCache(client, conf_zk_path, false)
      conf_node.start(true)
      conf_node.getListenable.addListener(new NodeCacheListener {
        def nodeChanged() = {
          val updatedata = new String(conf_node.getCurrentData().getData())
          val updateConf = parseConf(updatedata)
          LOG.info("before update config " + data)
          LOG.info("after update config " + updatedata)
          data = updatedata
          //when configuration changed,uppersist before configuration,then broadcast new configuration
          time_format.unpersist()
          id_conf.unpersist()
          time_format = ssc.sparkContext.broadcast(updateConf._1)
          id_conf = ssc.sparkContext.broadcast(updateConf._2)
        }
      }, pool)

      val accu = ssc.sparkContext.accumulator(0l)
      var pre = 0l
      def extract(topicData: String) = {
        val buf = new ArrayBuffer[String]
        accu += 1
        var hasException = false
        var json: JSONObject = null
        var data = topicData
        if (hasprefix) {
          data = topicData.substring(topicData.indexOf("}") + 1)
        }
        var id_mapping: HashMap[String, IdConf] = null
        var method = ""
        try {
          json = new JSONObject(data)
          method = json.getString("method")
          id_mapping = if (id_conf.value.get(method).isEmpty) null else id_conf.value.get(method).get
        } catch {
          case e1: JSONException => {
            LOG.error(exceptionToString(e1))
            hasException = true
          }
        }
        if (!hasException && null != id_mapping) {
          var ids: RawIds = null
          try {
            val method_time_format = time_format.value.get(method).get
            val timeField = method_time_format.timeField
            if (!method_time_format.hasField) {
              ids = new RawIds(System.currentTimeMillis() / 1000)
            } else {
              val format = method_time_format.format
              var timeStr = json.getString(timeField)
              import java.lang.Double
              import java.math.BigDecimal
              val b = new BigDecimal(timeStr);
              timeStr = b.toPlainString()
              if (format.equals("timestamp_s")) {
                ids = new RawIds(Double.valueOf(timeStr).toLong)
              } else if (format.equals("timestamp_ms")) {
                ids = new RawIds(Double.valueOf(timeStr).toLong / 1000)
              } else {
                ids = new RawIds(string2timeStamp(timeStr, format))
              }
            }
          } catch {
            case e: JSONException => {
              LOG.error(exceptionToString(e))
            }
          }
          if (null != ids) {
            id_mapping.foreach(idmapping => {
              try {
                val id_value = json.getString(idmapping._1)
                val conf = idmapping._2
                val channel_correlation = conf.channel_correlation
                var channel = conf.channel
                if (channel_correlation) {
                  channel = json.getString(channel)
                }
                val id = new Id(channel, conf.id_type, id_value)
                ids.addId(id)
              } catch {
                case e: JSONException => {
                  LOG.error(exceptionToString(e))
                }
              }
            })
            if (ids.size() > 0) {
              import scala.collection.mutable.Set
              import scala.collection.mutable.HashMap
              import scala.collection.JavaConversions._
              //when extract id size bigger than 0,query redis whether contain monitor id.
              BfdRedisPool.init(redis_address)
              val bfdjodis = BfdRedisPool.getInstance.getResource
              //抽取出的所有Id
              val id_set = ids.getIds().map { x => x.toString() }.toSet
              //监控Id集合
              val sid_set = Set[String]()
              //监控Id和关系映射
              val monitorid_Ids = new HashMap[String, Set[String]]
              //非监控Id
              val id_array = ArrayBuffer[String]()
              for (id <- id_set) {
                val monitor_key = "all:" + bussiness_id + ":" + id
                val m_ids = bfdjodis.smembers(monitor_key)
                if (!m_ids.isEmpty()) {
                  sid_set.add(id)
                  monitorid_Ids.put(id, m_ids.map { x => x.split(":", 2)(1) })
                } else {
                  id_array += id
                }
              }
              if (id_array.length > 0) {
                //batch query 
                val res = bfdjodis.mget(id_array.map { x => bussiness_id + ":" + x }.toSeq: _*)
                //新增关系
                val update = new ArrayBuffer[String]
                var i = 0
                for (id <- res) {
                  if (id != null) {
                    if (!monitorid_Ids.contains(id)) {
                      val key = "all:" + bussiness_id + ":" + id
                      val m_ids = bfdjodis.smembers(key)
                      if (!m_ids.isEmpty()) {
                        sid_set.add(id)
                        monitorid_Ids.put(id, m_ids.map { x => x.split(":", 2)(1) })
                      }
                    }
                    sid_set.add(id)
                  } else {
                    update += id_array(i)
                  }
                  i = i + 1
                }
                if (sid_set.size > 0) {
                  //监控Id所有关系集合
                  val tmp = monitorid_Ids.values.flatMap { x => x }.toSet
                  //根据id再次找到所有的sid
                  for (s <- tmp) {
                    val key = "all:" + bussiness_id + ":" + s
                    val m_ids = bfdjodis.smembers(key)
                    if (!m_ids.isEmpty()) {
                      sid_set.add(s)
                      monitorid_Ids.put(s, m_ids.map { x => x.split(":", 2)(1) })
                    }
                  }
                  val history = monitorid_Ids.values.flatMap { x => x }.toSet
                  //所有Id集合
                  val all_ids = history ++ id_set
                  val all_seq = all_ids.map { x => bussiness_id + ":" + x }.toSeq
                  val u_sid_set = sid_set.toSet
                  //为每一个Id输出数据
                  val out_id = u_sid_set -- id_set
                  for (id <- id_set) {
                    val new_ids = if (monitorid_Ids.contains(id)) {
                      val old = monitorid_Ids.get(id).get
                      val new_ids = all_ids -- old
                      new_ids
                    } else {
                      Set()
                    }
                    val data = genData(id, new_ids, topicData)
                    buf += data
                  }
                  for (id <- out_id) {
                    val new_ids = if (monitorid_Ids.contains(id)) {
                      val old = monitorid_Ids.get(id).get
                      val new_ids = all_ids -- old
                      new_ids
                    } else {
                      Set()
                    }
                    val data = genData(id, new_ids, "")
                    buf += data
                  }
                  //将所有Id添加到监控Id关系中,必须全量，存在关系合并情况
                  for (sid <- sid_set) {
                    bfdjodis.sadd("all:" + bussiness_id + ":" + sid, all_seq: _*)
                  }
                  //为新增Id添加映射关系
                  if (update.size > 0) {
                    //取最小的监控Id
                    val small_sid = sid_set.toList.sorted
                    val small = small_sid(0)
                    for (u_id <- update) {
                      bfdjodis.set(bussiness_id + ":" + u_id, small)
                      if (set_redis_timeout) {
                        bfdjodis.expire(bussiness_id + ":" + u_id, time_out)
                      }
                    }
                  }
                }
              } else {
                val tmp = monitorid_Ids.values.flatMap { x => x }.toSet
                val other_sid=Set[String]()
                for (s <- tmp) {
                  val key = "all:" + bussiness_id + ":" + s
                  val m_ids = bfdjodis.smembers(key)
                  if (!m_ids.isEmpty()) {
                    other_sid.add(s)
                    monitorid_Ids.put(s, m_ids.map { x => x.split(":", 2)(1) })
                  }
                }
                val history = monitorid_Ids.values.flatMap { x => x }.toSet
                val all = history.map { x => bussiness_id + ":" + x }.toSeq
                for (sid <- sid_set) {
                  bfdjodis.sadd("all:" + bussiness_id + ":" + sid, all: _*)
                  val old = monitorid_Ids.get(sid).get.toSet
                  val n_id = history -- old
                  val data = genData(sid, n_id, topicData)
                  buf += data
                }
                val other=other_sid--sid_set
                for (sid <- other) {
                   bfdjodis.sadd("all:" + bussiness_id + ":" + sid, all: _*)
                  val old = monitorid_Ids.get(sid).get.toSet
                  val n_id = history -- old
                  val data = genData(sid, n_id, "")
                  buf += data
                }
              }
              BfdRedisPool.getInstance.returnResourceObject(bfdjodis)
            }
          }
        }
        buf
      }
      //get accumulator value every iterval second,when switch topic,we can determine whether daily topic has data

      thread = new Thread() {
        override def run() {
          while (!Thread.interrupted()) {
            if (accu.value - pre == 0) changed = false else changed = true
            pre = accu.value
            LOG.info("monitor thread accu size is " + pre)
            TimeUnit.SECONDS.sleep(iterval_second)
          }
        }
      }
      thread.start()

      //conf kafka
      val kafkaParams: Map[String, String] = Map("group.id" -> kafka_group, "auto.offset.reset" -> "smallest", "zookeeper.connect" -> kafka_zk_address,
        "zookeeper.session.timeout.ms" -> "10000", "zookeeper.sync.time.ms" -> "200", "auto.commit.interval.ms" -> "1000")
      val topic = getTopic()
      val topicMap = Map(topic -> num_of_thread)
      //make connection to kafka
      val lines = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER).map(_._2)

      val monitor_data = lines.flatMap { line => extract(line) }

      monitor_data.foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          KafkaProducerPool.init(request_required_acks, producer_type, metadata_broker_list)
          val producer = KafkaProducerPool.getInstance
          partition.foreach(data => {
            val kafka_data = new KeyedMessage[String, String](monitor_topic, data);
            producer.send(kafka_data)
          })
        })
      })
      ssc.start()
      ssc.awaitTermination()
    }

    //background thread for topic switch

    class SwitchTask extends TimerTask {
      override def run() {
        LOG.info("stop last day")
        stopLastDay()
        LOG.info("register task")
        registerTask()
        LOG.info("start ssc")
        startStreamContext()
      }
    }
    def registerTask() {
      timer = new Timer()
      schedule = new TimeSchedule(timer)
      schedule.addFixedTask(hour, minute, 0, new SwitchTask)
    }
    registerTask
    startStreamContext()
  }
  //determine whether history data contain given id

  //split ids in every record,to solve distribute transaction
  def split(ids: RawIds) = {
    val res = ArrayBuffer[(String, RawIds)]()
    for (id <- ids.idSet) {
      res += ((id.toString(), ids))
    }
    res
  }
  //parse time format to timestamp
  def string2timeStamp(date: String, format: String) = {
    try {
      val sdf = new SimpleDateFormat(format);
      val dat = sdf.parse(date);
      dat.getTime() / 1000;
    } catch {
      case e: ParseException => {
        LOG.error(exceptionToString(e))
        System.currentTimeMillis() / 1000
      }
    }
  }

  //parse zookeeper configuration
  def parseConf(data: String) = {
    val time_format = new HashMap[String, TimeFormat]()
    val id_conf = new HashMap[String, HashMap[String, IdConf]]()
    val json = new JSONObject(data)
    val topic_type = json.getBoolean("switchFlag")
    val methodsArray = new JSONArray(json.getString("methods"))
    for (i <- 0 until methodsArray.length()) {
      val methodobj = methodsArray.getJSONObject(i)
      val method = methodobj.getString("method")
      var timeField = ""
      val hastimeField = methodobj.has("timeField")
      val format = methodobj.getString("timeStyle")
      if (hastimeField) {
        timeField = methodobj.getString("timeField")
      }
      time_format.put(method, new TimeFormat(format, hastimeField, timeField))
      val idsArray = methodobj.getJSONArray("ids")
      val methodMap = new HashMap[String, IdConf]
      id_conf.put(method, methodMap)
      parseIdsConf(methodMap, idsArray)
    }
    (time_format, id_conf)
  }
  //parse id configuration
  def parseIdsConf(id_conf: HashMap[String, IdConf], idsArray: JSONArray) {
    for (i <- 0 until idsArray.length()) {
      val id = idsArray.getJSONObject(i)
      val channel = id.getString("channel")
      val id_type = id.getString("type")
      val idField = id.getString("id")
      val dynamic = id.getString("dynamic")
      import java.lang.Boolean
      id_conf.put(idField, new IdConf(channel, id_type, Boolean.valueOf(dynamic)))
    }
  }
  //exception to string

  def exceptionToString(e: Throwable) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw, true)
    e.printStackTrace(pw);
    pw.flush()
    sw.flush()
    sw.toString()
  }

}