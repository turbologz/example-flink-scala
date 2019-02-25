package org.apache.flink.streams

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class SpaceAppsParserStream {

  def parse(stream: DataStream[CloudFoundryLog]): DataStream[(String, String, String, String, Int)] = {
    stream
      .map(cf => (cf.host.split('.'), cf.ident))
      .map((split: (Array[String], String)) => (split._1(0), split._1(1), split._1(2), split._2, 1))
      .keyBy(0, 1, 2, 3)
      .timeWindow(Time.seconds(10))
      .sum(4)
  }
}
