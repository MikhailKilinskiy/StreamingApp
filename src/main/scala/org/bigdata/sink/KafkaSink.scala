package org.bigdata.sink

import java.text.SimpleDateFormat

import org.apache.kafka.clients.producer._

import scala.concurrent.{Future, Promise}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{rdd, _}
import org.apache.spark.streaming.kafka010._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.bigdata.configs.Configuration
import org.bigdata.models.StockDataGen
import org.bigdata.streaming.Streaming
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object KafkaSink {

  private lazy val kafkaProducer: KafkaProducer[String, String] = {
    val props = Configuration.producerProps
    val producer = new KafkaProducer[String, String](props)

    producer
  }

  private val topic: String = Configuration.sinkTopicName

  def saveToKafka(o: String): Unit =
    kafkaProducer.send(new ProducerRecord[String, String](topic, o))

  def extractData(event: String): StockDataGen = {
    // Formatting JSON date
    implicit val formats = new DefaultFormats {
      override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
    val json = parse(event)
    json.extract[StockDataGen]
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = Configuration.sparkConf
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val checkpointPath = "/tmp"
    ssc.checkpoint(checkpointPath)

    val streamingContext = new Streaming(ssc)
    val stream = streamingContext.stream

    stream.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          partition.foreach{event =>

            val stockData = extractData(event.value())
            val value = s"${stockData.symbol.toString}:${stockData.price.toString}"
            println(value)
            saveToKafka(value)

          }
        })
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) // Exactly-once semantic
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
