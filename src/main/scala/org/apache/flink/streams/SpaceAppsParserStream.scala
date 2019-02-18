package org.apache.flink.streams

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.cf.CloudFoundryLog

class SpaceAppsParserStream {

  def parse(stream: Stream[String]) {
    stream
      .map((data) =>
        (new ObjectMapper() with ScalaObjectMapper)
          .registerModule(DefaultScalaModule)
          .readValue(data, classOf[CloudFoundryLog])
      )
  }
}
