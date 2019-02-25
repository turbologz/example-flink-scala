package org.apache.flink.streams

import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import org.apache.flink.utils.Utils

class CfLogParserStream {

  def parse(stream: DataStream[String]): DataStream[CloudFoundryLog] = {
    stream
      .map(json => new Utils().parseJson[CloudFoundryLog](json))
  }
}
