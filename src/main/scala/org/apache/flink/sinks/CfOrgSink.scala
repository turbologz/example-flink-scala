package org.apache.flink.sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.cf.{CloudFoundryOrg, CloudFoundrySpace}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.utils.Utils

class CfOrgSink(cfApplications: CfApplications) {

  private val self = this

  def getSink(): SinkFunction[(String, String, String, String, Int)] = {

    new SinkFunction[(String, String, String, String, Int)]() {
      def invoke(value: (String, String, String, String, Int)) {
        self.createOrg(value._1)
      }
    }
  }

  def createOrg(name: String): CloudFoundryOrg = {
    new Utils().parseJson[CloudFoundryOrg](this.cfApplications.createOrg(name))
  }

  def createSpace(orgId: String, name: String): CloudFoundrySpace = {
    new Utils().parseJson[CloudFoundrySpace](this.cfApplications.createSpace(orgId, name))
  }
}
