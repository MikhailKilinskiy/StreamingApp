package org.bigdata.source

import org.apache.kafka.clients.producer._
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.bigdata.configs.Configuration
import org.bigdata.models.StockDataGen
import org.bigdata.source.DataGenerator



class DataProducer {

  private val kafkaProducer: KafkaProducer[String, String] = {
    val props = Configuration.producerProps
    val producer = new KafkaProducer[String, String](props)

    producer
  }

  private val topic: String = Configuration.topicName

  def save(o: String): Unit =
    kafkaProducer.send(new ProducerRecord[String, String](topic, o))
}

object DataProducer {
  def main(args: Array[String]): Unit = {
    val eventProducer = new DataProducer
    val rate = 1
    val wait = (1000/rate)
    val jsonMapper = new ObjectMapper()
    jsonMapper.registerModule(DefaultScalaModule)

    while(true) {
      var data = DataGenerator.GenerateData()

      val jsonData = jsonMapper.writeValueAsString(data)

      Thread.sleep(wait)
      println(jsonData)
      eventProducer.save(jsonData)
    }
  }
}
