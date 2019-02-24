package org.apache.flink.sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.cf.CloudFoundryOrg
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.utils.Utils

class CfOrgSink {

  def getSink(): SinkFunction[(String, String, String, Int)] = {
    new SinkFunction[(String, String, String, Int)]() {
      def invoke(value: (String, String, String, Int)) {

        print(s"Sending org to sink ${value._1}")

        new Utils().parseJson[CloudFoundryOrg](new CfApplications().createOrg(value._1).data.text)
      }
    }
  }
}
