package org.apache.flink.streams

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class SpaceAppsParserStream {

  def parse(stream: DataStream[CloudFoundryLog]): DataStream[(String, String, Int)] = {
    stream
      .map {
        _.host.split('.')
      }
      .map(split => (split(1), split(2), 1))
      .keyBy(0, 1)
      .timeWindow(Time.seconds(10))
      .sum(2)
  }
}