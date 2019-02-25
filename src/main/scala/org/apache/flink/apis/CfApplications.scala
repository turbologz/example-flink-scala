package org.apache.flink.apis

import ujson._

class CfApplications {

  private val applicationsUrl = "http://prod-turbo-logz-applications-api"
  private val headers = Map("Content-Type" -> "application/json")

  def createOrg(name: String): String = {
    requests.post(s"$applicationsUrl/orgs", headers = headers, data = Obj("name" -> name)
      .render())
      .data
      .text
  }

  def createSpace(orgId: String, name: String): String = {
    requests.post(s"$applicationsUrl/spaces", headers = headers, data = Obj("orgId" -> orgId, "name" -> name)
      .render())
      .data
      .text
  }

  def createApp(spaceId: String, appId: String, name: String): String = {
    requests.post(s"$applicationsUrl/apps", headers = headers, data = Obj("spaceId" -> spaceId, "appId" -> appId, "name" -> name)
      .render())
      .data
      .text
  }
}
