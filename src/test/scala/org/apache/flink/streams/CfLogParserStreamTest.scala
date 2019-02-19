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

class CfLogParserStreamTest extends AbstractTestBase {

  val cloudFoundryLog = new CloudFoundryLog(
    "thor.prod.prod-blog-backend",
    "abc123",
    "\u001B[34mℹ\u001B[39m \u001B[90m｢wdm｣\u001B[39m: Compiled successfully.\n",
    1234,
    "[RTR/11]",
    "cf.app.user.info",
    null,
    null
  )

  @Test
  def `should parse string from cloud foundry logs to a json object`(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new SourceFunction[String]() {
      def run(ctx: SourceFunction.SourceContext[String]) {
        ctx.collect("{\"host\":\"thor.prod.prod-blog-backend\",\"ident\":\"abc123\",\"message\":\"\\u001b[34mℹ\\u001b[39m \\u001b[90m｢wdm｣\\u001b[39m: Compiled successfully.\\n\",\"time\":1234,\"pid\":\"[RTR/11]\",\"tag\":\"cf.app.user.info\"}")
      }

      override def cancel(): Unit = {}
    })

    new CfLogParserStream()
      .parse(stream)
      .addSink(new SinkFunction[CloudFoundryLog]() {
        def invoke(value: CloudFoundryLog) {
          JSONCollectSink.testResults += value
        }
      })

    env.execute("Parse Cloud Foundry Log Test")

    assertEquals(cloudFoundryLog.host, JSONCollectSink.testResults.head.host)
    assertEquals(cloudFoundryLog.ident, JSONCollectSink.testResults.head.ident)
    assertEquals(cloudFoundryLog.message, JSONCollectSink.testResults.head.message)
    assertEquals(cloudFoundryLog.time, JSONCollectSink.testResults.head.time)
    assertEquals(cloudFoundryLog.pid, JSONCollectSink.testResults.head.pid)
    assertEquals(cloudFoundryLog.tag, JSONCollectSink.testResults.head.tag)
    assertEquals(cloudFoundryLog.extradata, JSONCollectSink.testResults.head.extradata)
    assertEquals(cloudFoundryLog.msgid, JSONCollectSink.testResults.head.msgid)
  }
}

object JSONCollectSink {
  val testResults: mutable.MutableList[CloudFoundryLog] = mutable.MutableList[CloudFoundryLog]()
}
