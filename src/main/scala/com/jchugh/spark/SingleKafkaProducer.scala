package com.jchugh.spark

import java.util

import org.apache.kafka.clients.producer.{ProducerRecord, KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

/**
 * Created by jchugh on 6/8/15.
 */
class SingleKafkaProducer private(bootstrapServers: util.ArrayList[String]) {
  val kafkaProducerParams = new util.HashMap[String, java.lang.Object]()
  kafkaProducerParams.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  val kp = new KafkaProducer[String, String](kafkaProducerParams, new StringSerializer, new StringSerializer)
  def send(topic: String, value: String): Unit = {
    kp.send(new ProducerRecord[String, String](topic, value))
  }
}

object SingleKafkaProducer {
  private val bsServers = new util.ArrayList[String]()
  bsServers.add("localhost:9092")
  private val producer = new SingleKafkaProducer(bsServers)
  var getProducer: SingleKafkaProducer = producer
}
