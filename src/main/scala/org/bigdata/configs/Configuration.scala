package org.bigdata.configs

import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.{StringSerializer,StringDeserializer}
import org.apache.spark.SparkConf

object Configuration {
  private val confPath = "/home/makilins/IdeaProjects/org.bigdata/src/main/resources/application.conf"
  private val conf = ConfigFactory
    .parseFile(new File("/home/makilins/IdeaProjects/org.bigdata/src/main/resources/application.conf"))
    .getConfig("org")

  def batchDuration = conf.getDuration("batch.duration").toMillis

  def sparkConf:SparkConf = {
    val name = conf.getConfig("spark").getString("app.name")
    val cfg = new SparkConf()
      .setMaster("local[*]")
      .setAppName(name)
      .set("enable.auto.commit", "false")
      .set("spark.streaming.kafka.consumer.cache.enabled", "false")

    cfg
  }

  def topicName: String = conf.getString("source.topic.name")


  def schemaRegistryUrl: String = conf.getConfig("kafka.consumer").getString("schema.registry.url")


  def producerProps: Properties = {
    val kafkaConf =  conf.getConfig("kafka.producer")
    val props = new Properties()

    props.put("bootstrap.servers", kafkaConf.getString("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[StringSerializer])

    props

  }

  def consumerProps: Map[String, Object] = {
    val kafkaConf =  conf.getConfig("kafka.consumer")
    val props = new Properties()

    Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConf.getString("bootstrap.servers"),
      ConsumerConfig.GROUP_ID_CONFIG -> kafkaConf.getString("group.id"),
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

  }

}
