package org.apache.flink.sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class CfOrgSink {

  def getSink(): SinkFunction[(String, String, String, Int)] = {
    new SinkFunction[(String, String, String, Int)]() {
      def invoke(value: (String, String, String, Int)) {

        new CfApplications().createOrg(value._1).body

      }
    }
  }
}
