package org.apache.flink.apis

import ujson._

class CfApplications {

  private val applicationsUrl = "http://prod-turbo-logz-applications-api"
  private val headers = Map("Content-Type" -> "application/json")

  def createOrg(name: String): requests.Response = {

    requests.post(s"$applicationsUrl/orgs", headers = headers, data = Obj(
      "name" -> name
    ).render())

  }
}
