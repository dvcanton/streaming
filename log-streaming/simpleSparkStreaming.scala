package com.logstreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext

object simpleSparkStreaming extends Any {


  val interval = Duration(5000) // 5 seconds
  val logsDirectory = "log/"

  // Create Spark Conf and Spark Streaming Context.
  val sparkConf = new SparkConf().setAppName("Simple spark streaming app")
  val streamingContext = new StreamingContext(sparkConf, Seconds(interval))

  val logData: DStream[String] = streamingContext.textFileStream(logsDirectory)

  // Create DStream
  val accessLogsDStream: DStream[Log] = logData
    .flatMap(line => Log.parseLogLine(line).iterator)
    .cache()
}
