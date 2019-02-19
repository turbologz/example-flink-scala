package org.apache.flink.streams

import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class SpaceAppsParserStream {

  def parse(stream: DataStream[CloudFoundryLog]): DataStream[(String, String)] = {
    stream
      .map {
        _.host.split('.')
      }
      .map(split => (split(1), split(2)))
  }
}
