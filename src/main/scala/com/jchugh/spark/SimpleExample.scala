package com.jchugh.spark

import java.util
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{ProducerRecord, ProducerConfig, KafkaProducer}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.{HashPartitioner, SparkConf}
import scala.collection.mutable.HashSet

/**
 * Created by jchugh on 5/31/15.
 * much help from http://apache-spark-user-list.1001560.n3.nabble.com/Writing-to-RabbitMQ-td11283.html
 */
object SimpleExample {
  def main (args: Array[String]): Unit = {

    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.sum

      val previousCount = state.getOrElse(0)

      Some(currentCount + previousCount)
    }

    val ordinary=(('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet

    val newUpdateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
      iterator.flatMap(t => updateFunc(t._2, t._3).map(s => (t._1, s)))
    }

    /**
     * Extracts the hashtags out of a tweet (english only)
     * @param tweet the tweet as [[String]]
     * @return [[HashSet]] of [[String]] containing the hashtags
     */
    def hashTagCleanFunc (tweet: String): HashSet[String] = {
      var x = new StringBuilder
      val y = HashSet[String]()
      var inHash = false
      for (c <- tweet) {
        if (inHash && !ordinary.contains(c)) {
          if (x.length > 1) y += x.toString()
          inHash = false
          x = new StringBuilder
        } else if (inHash && ordinary.contains(c)) {
          x += c.toUpper
        }
        if (c == '#') {
          x += c
          inHash = true
        }
      }
      if (x.length > 1) {
        y += x.toString()
      }
      return y
    }

    val sparkConf = new SparkConf().setAppName("KafkaTwitterTrending").setSparkHome("/usr/scala")
    sparkConf.setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://localhost:9000/user/spark/checkpoint/")
    val kafkaParams = Map[String, String](("metadata.broker.list" -> "localhost:9092"), ("zookeeper.connect", "localhost:2181"))
    val topicsSet = "test-topic".split(",").toSet
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)
    val words = lines.flatMap(hashTagCleanFunc)
    val wordDstream = words.map(x => (x, 1))

    val stateDstream = wordDstream.updateStateByKey[Int](newUpdateFunc,
      new HashPartitioner (ssc.sparkContext.defaultParallelism), true).cache()
    var counter = 0
    stateDstream.foreachRDD(r => {
      val producer = SingleKafkaProducer.getProducer
      counter += 1

      r.foreach(u => {
      })

      if (counter >= 20) {
        producer.send("trending", r.sortBy(_._2, false).take(10).mkString(","))
        counter = 0
      } else {
        producer.send("trending", r.sortBy(_._2, false).collect().mkString(","))
      }

    })
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }


}
