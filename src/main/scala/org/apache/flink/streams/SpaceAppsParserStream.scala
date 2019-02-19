package org.apache.flink.streams

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class SpaceAppsParserStream {

  def parse(stream: DataStream[CloudFoundryLog]): DataStream[List[(String, String)]] = {
    stream
      .map {
        _.host.split('.')
      }
      .map(split => (split(1), split(2)))
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.SECONDS))
      .apply(new UniqueWindowFunction())
  }
}

class UniqueWindowFunction() extends WindowFunction[String, String, Iterable[(String, String)], TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, String)], out: Collector[String]): Unit = {
    val discount = input.map(t => t._2).toSet.size
    out.collect(s"Distinct elements: $discount")
  }
}
