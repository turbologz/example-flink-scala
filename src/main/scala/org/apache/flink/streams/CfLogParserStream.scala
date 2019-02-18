package org.apache.flink.streams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._

class CfLogParserStream {

  def parse(stream: DataStream[String]): DataStream[CloudFoundryLog] = {
    stream
      .map((data) =>
        (new ObjectMapper() with ScalaObjectMapper)
          .registerModule(DefaultScalaModule)
          .readValue(data, classOf[CloudFoundryLog])
      )
  }
}
