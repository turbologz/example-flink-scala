package org.apache.flink

import org.apache.flink.api.scala._
import org.apache.flink.apis.CfApplications
import org.apache.flink.sinks.CfOrgSink
import org.apache.flink.sources.KafkaSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streams.{CfLogParserStream, SpaceAppsParserStream}

object CfSpacesAppsParser {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.addSource(new KafkaSource().getSource(env, "log-analysis", "cf-spaces-apps-parser"))

    new SpaceAppsParserStream()
      .parse(new CfLogParserStream().parse(stream))
      .addSink(new CfOrgSink(new CfApplications))

    env.execute()
  }

}
