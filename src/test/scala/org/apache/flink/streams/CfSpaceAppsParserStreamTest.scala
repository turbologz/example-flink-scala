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
  def `should get names of spaces and apps`(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
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

        ctx.collect(new CloudFoundryLog(
          "thor.prod.prod-blog-backend1",
          "abc123",
          "\u001B[34mℹ\u001B[39m \u001B[90m｢wdm｣\u001B[39m: Compiled successfully.\n",
          1234,
          "[RTR/11]",
          "cf.app.user.info",
          null,
          null
        ))

        ctx.collect(new CloudFoundryLog(
          "thor.prod2.prod-blog-backend2",
          "abc123",
          "\u001B[34mℹ\u001B[39m \u001B[90m｢wdm｣\u001B[39m: Compiled successfully.\n",
          1234,
          "[RTR/11]",
          "cf.app.user.info",
          null,
          null
        ))

        ctx.collect(new CloudFoundryLog(
          "thor.prod2.prod-blog-backend2",
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
      .addSink(new SinkFunction[(String, String, Int)]() {
        def invoke(value: (String, String, Int)) {
          SpacesAppsCollectSink.testResults += value
        }
      })

    env.execute("Parse Cloud Foundry Log Test")

    val expected: List[(String, String, Int)] = List(
      ("prod", "prod-blog-backend", 1),
      ("prod2", "prod-blog-backend2", 2),
      ("prod", "prod-blog-backend1", 1)
    )

    assertEquals(expected, SpacesAppsCollectSink.testResults.toList)
  }

}

object SpacesAppsCollectSink {
  val testResults: mutable.MutableList[(String, String, Int)] = mutable.MutableList[(String, String, Int)]()
}
