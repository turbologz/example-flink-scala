package org.apache.flink.streams

import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class SpaceAppsParserStream {

  def parse(stream: DataStream[CloudFoundryLog]): DataStream[(String, String)] = {
    stream
      .map(cloudFoundryLog => this.splitHost(cloudFoundryLog))
      .map(split => (split(0), split(1)))
  }

  def splitHost(cloudFoundryLog: CloudFoundryLog): Array[String] = {
    cloudFoundryLog.host.split(".")
  }
}
