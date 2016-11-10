package com.bfd.pool

import kafka.javaapi.producer.Producer
import kafka.producer.ProducerConfig
import java.util.Properties

object KafkaProducerPool {
  @volatile private[this] var inited = false
  @volatile private[this] var instanced = false
  private[this] var request_required_acks: String = _
  private[this] var producer_type: String = _
  private[this] var metadata_broker_list: String = _
  private[this] var instance: Producer[String, String] = _
  def init(acks: String, producer_type: String, broker_list: String) {
    if (!inited) {
      this.synchronized {
        if (!inited) {
          this.request_required_acks = acks
          this.producer_type = producer_type
          this.metadata_broker_list = broker_list
          inited = true
        }
      }
    }
  }
  def getInstance() = {
    if (!instanced) {
      this.synchronized {
        if (!instanced) {
          val props = new Properties()
          props.put("metadata.broker.list", metadata_broker_list)
          props.put("serializer.class", "kafka.serializer.StringEncoder")
          props.put("request.required.acks", request_required_acks)
          props.put("producer.type", producer_type)
          val config = new ProducerConfig(props)
          instance = new Producer[String, String](config)
          val thread = new Thread() {
            override def run() = {
              instance.close
              instance = null
            }
          };
          Runtime.getRuntime().addShutdownHook(thread);
          instanced = true
        }
      }
    }
    instance
  }
}