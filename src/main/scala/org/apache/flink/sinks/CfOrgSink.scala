package org.apache.flink.sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.cf.{CloudFoundryApp, CloudFoundryOrg, CloudFoundrySpace}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.utils.Utils

class CfOrgSink(cfApplications: CfApplications) extends SinkFunction[(String, String, String, String, Int)] {

  def invoke(value: (String, String, String, String, Int)) {
    this.createData(value._1, value._2, value._3, value._4)
  }

  def createData(orgName: String, spaceName: String, appName: String, ident: String): CloudFoundryApp = {
    val org = this.createOrg(orgName)

    val space = this.createSpace(org.id, spaceName)

    this.createApp(space.id, ident, appName)
  }

  private def createOrg(name: String): CloudFoundryOrg = {
    new Utils().parseJson[CloudFoundryOrg](this.cfApplications.createOrg(name))
  }

  private def createSpace(orgId: String, name: String): CloudFoundrySpace = {
    new Utils().parseJson[CloudFoundrySpace](this.cfApplications.createSpace(orgId, name))
  }

  private def createApp(spaceId: String, appId: String, name: String): CloudFoundryApp = {
    new Utils().parseJson[CloudFoundryApp](this.cfApplications.createApp(spaceId, appId, name))
  }
}
