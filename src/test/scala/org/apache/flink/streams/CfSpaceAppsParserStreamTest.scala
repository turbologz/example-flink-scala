package org.apache.flink.streams

import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class CfSpaceAppsParserStreamTest extends AbstractTestBase {

  @Test
  def `should parse string from cloud foundry logs to a json object`(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new SourceFunction[CloudFoundryLog]() {
      def run(ctx: SourceFunction.SourceContext[CloudFoundryLog]) {
        ctx.collect(new CloudFoundryLog(
          "thor.prod.prod-blog-backend",
          "abc123",
          "\u001B[34mℹ\u001B[39m \u001B[90m｢wdm｣\u001B[39m: Compiled successfully.\n",
          1234,
          "[RTR/11]",
          "cf.app.user.info",
          null,
          null
        ))
      }

      override def cancel(): Unit = {}
    })

    new SpaceAppsParserStream()
      .parse(stream)
      .addSink(new SinkFunction[(String, String)]() {
        def invoke(value: (String, String)) {
          CollectSink.testResults += value
        }
      })

    env.execute("Parse Cloud Foundry Log Test")

    assertEquals(("prod", "prod-blog-backend"), CollectSink.testResults.head)
  }

  object CollectSink {
    val testResults: mutable.MutableList[(String, String)] = mutable.MutableList[(String, String)]()
  }
}
