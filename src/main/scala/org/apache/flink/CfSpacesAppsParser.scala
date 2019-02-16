package org.apache.flink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object CfSpacesAppsParser {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "prod-messaging-kafka:9092")
    properties.setProperty("group.id", "cf-spaces-apps-parser")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("log-analysis", new SimpleStringSchema, properties))

    stream
      .map(data =>
        (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule).readValue(data, classOf[CloudFoundryLog])
      )
      .flatMap(cfLog => cfLog.host.split("."))
      .map(word => word.toLowerCase)
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.SECONDS))
      .sum(1)
      .print()

    env.execute()
  }

}
