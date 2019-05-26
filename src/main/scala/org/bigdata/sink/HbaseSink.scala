package org.bigdata.sink

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.bigdata.configs.Configuration
import org.bigdata.models.StockDataGen
import org.bigdata.storage.HbaseStorage
import org.bigdata.streaming.Streaming
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JsonDSL}

object HbaseSink {

  def extractData(event: String): StockDataGen = {
    // Formatting JSON date
    implicit val formats = new DefaultFormats {
      override def dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
    val json = parse(event)
    json.extract[StockDataGen]
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    val jsonConfig = JsonDSL

    val sparkConf = Configuration.sparkConf
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    //val checkpointPath = "/tmp"
    //ssc.checkpoint(checkpointPath)

    val streamingContext = new Streaming(ssc)
    val stream = streamingContext.stream
    val hbaseStorage = new HbaseStorage("Test", "StockData", "Price")

    stream.foreachRDD{rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      if(!rdd.isEmpty()) {
        rdd.foreachPartition(partition => {
          partition.foreach{event =>

            val stockData = extractData(event.value())
            val symbol = stockData.symbol.toString
            val price = stockData.price.toString
            println(symbol, price)

            hbaseStorage.update(symbol, price)

          }
        })
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) // Exactly-once semantic
    }

    ssc.start()
    ssc.awaitTermination()

    hbaseStorage.close()

  }

}

