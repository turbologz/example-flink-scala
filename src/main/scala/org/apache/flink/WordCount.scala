package org.apache.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time

object WordCount {

  type WordCount = (String, Int)

  def countWords(lines: DataStream[String], window: Time): DataStream[WordCount] = {
    lines
      .flatMap(line => line.split(" "))
      .filter(word => !word.isEmpty)
      .map(word => word.toLowerCase)
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(window)
      .sum(1)
  }

}