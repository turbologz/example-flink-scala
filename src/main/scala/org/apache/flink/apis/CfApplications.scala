package org.apache.flink.apis

class CfApplications {

  private val applicationsUrl = "http://prod-turbo-logz-applications-api"
  private val headers = Map("Content-Type" -> "application/json")

  def createOrg(name: String): requests.Response = {

    requests.post(s"$applicationsUrl/orgs", headers = headers, data = Map("name" -> name))

  }
}
