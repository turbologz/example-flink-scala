import java.util.concurrent.TimeUnit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.flink.api.scala._
import org.apache.flink.cf.CloudFoundryLog
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class ExampleTest extends AbstractTestBase {

  @Test
  def testReduceWindow(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new SourceFunction[(String)]() {
      def run(ctx: SourceFunction.SourceContext[(String)]) {
        ctx.collect("{\"host\":\"thor.prod.prod-blog-backend\",\"ident\":\"abc123\",\"message\":\"\\u001b[34mℹ\\u001b[39m \\u001b[90m｢wdm｣\\u001b[39m: Compiled successfully.\\n\",\"time\":1234,\"pid\":\"[RTR/11]\",\"tag\":\"cf.app.user.info\"}")
      }

      override def cancel(): Unit = {}
    })

    stream
      .map(data =>
        (new ObjectMapper() with ScalaObjectMapper).registerModule(DefaultScalaModule).readValue(data, classOf[CloudFoundryLog])
      )
      .flatMap(cfLog => cfLog.host.split("."))
      .map(word => (word, 1))
      .keyBy(0)
      .timeWindow(Time.of(10, TimeUnit.SECONDS))
      .sum(1)
      .addSink(new SinkFunction[(String)]() {
        def invoke(value: (String)) {
          ExampleTest.testResults += value.toString
        }
      })

    env.execute("Reduce Window Test")

    val expectedResult = mutable.MutableList(
      "(aaa,3)",
      "(aaa,21)",
      "(bbb,12)"
    )

    assertEquals(expectedResult.sorted, ExampleTest.testResults.sorted)
  }
}

object ExampleTest {

  val testResults = mutable.MutableList[String]()
}