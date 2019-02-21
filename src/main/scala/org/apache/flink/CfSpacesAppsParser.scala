package org.apache.flink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streams.{CfLogParserStream, SpaceAppsParserStream}

object CfSpacesAppsParser {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "prod-messaging-kafka:9092")
    properties.setProperty("group.id", "cf-spaces-apps-parser")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("log-analysis", new SimpleStringSchema, properties))

    new SpaceAppsParserStream()
      .parse(new CfLogParserStream().parse(stream))
      .print()

    env.execute()
  }

}
