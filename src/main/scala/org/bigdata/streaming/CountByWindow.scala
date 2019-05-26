package org.bigdata.streaming

import org.apache.spark.streaming.{rdd, _}
import org.apache.spark.streaming.kafka010._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

import org.bigdata.configs.Configuration
import org.bigdata.streaming.Streaming
import org.bigdata.models.StockDataGen

import java.text.SimpleDateFormat

import org.json4s._
import org.json4s.jackson.JsonMethods._

object CountByWindow {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = Configuration.sparkConf
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val checkpointPath = "/tmp"
    ssc.checkpoint(checkpointPath)

    val stream = new Streaming(ssc).stream

    // implicit val formats = DefaultFormats // Необходимо для парсинга json в объект

    val stockStream = stream
      .map(
        rec => {implicit val formats = new DefaultFormats {
          override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        };
          parse(rec.value().toString).extract[StockDataGen]}
      )
      .filter(_.name == "Apple")
      .map(rec => rec.name)
      .countByWindow(Seconds(5), Seconds(1))
      .print


    ssc.start
    ssc.awaitTermination()

  }


}
