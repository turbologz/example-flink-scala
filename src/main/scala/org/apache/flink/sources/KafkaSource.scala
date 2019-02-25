package org.apache.flink.sources

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

class KafkaSource {

  def getSource(env: StreamExecutionEnvironment, topic: String, groupId: String): FlinkKafkaConsumer[String] = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", "prod-messaging-kafka:9092")
    properties.setProperty("group.id", groupId)

    new FlinkKafkaConsumer[String](topic, new SimpleStringSchema, properties)
  }
}
