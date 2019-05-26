package org.bigdata.streaming

import org.apache.spark.streaming.{rdd, _}
import org.apache.spark.streaming.kafka010._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

import org.bigdata.configs.Configuration
import org.bigdata.streaming.Streaming


object StreamingAccumulator {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = Configuration.sparkConf
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val checkpointPath = "/tmp"
    ssc.checkpoint(checkpointPath)

    val sc = ssc.sparkContext

    val stream = new Streaming(ssc).stream

    val accum = sc.longAccumulator("Accumulator")

    stream
      .map(_ => accum.add(1))
      .foreachRDD{rdd =>
        rdd.collect() // Необходимо для выполнения map
        println("Total stock price query: " + accum.sum)
      }

    ssc.start
    ssc.awaitTermination()

  }

}

