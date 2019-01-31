/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object WordCountKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties
    properties.setProperty("bootstrap.servers", "test-messaging-kafka:9092")
    properties.setProperty("group.id", "log-word-analysis")

    val stream = env.addSource(new FlinkKafkaConsumer[String]("log-analysis", new SimpleStringSchema, properties))

    stream
      .flatMap(line => line.split(" "))
      .filter(word => !word.isEmpty)
      .map(word => word.toLowerCase)
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.SECONDS))
      .sum(1)
      .print()

    env.execute()
  }

}