package org.bigdata.streaming

import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

import org.bigdata.configs.Configuration


class Streaming (ssc: StreamingContext) {
  //type RawRecord = (String, String)

  private val kafkaParams = Configuration.consumerProps

  val stream = KafkaUtils.createDirectStream[String, String](
    ssc,
    LocationStrategies.PreferConsistent,
    ConsumerStrategies.Subscribe[String, String](Configuration.topicName.split(",").toSet, kafkaParams)
  )
}
