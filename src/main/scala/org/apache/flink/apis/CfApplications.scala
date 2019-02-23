package org.apache.flink.apis

import com.softwaremill.sttp._
import com.softwaremill.sttp.json4s.{asJson, _}
import org.apache.flink.cf.CloudFoundryOrg

import scala.concurrent.Future

class CfApplications {

  val applicationsUrl = "http://prod-turbo-logz-applications-api"

  def createOrg(name: String): Future[Response[CloudFoundryOrg]] = {

    implicit val backend = HttpURLConnectionBackend()
    implicit val serialization = org.json4s.native.Serialization

    val request = sttp
      .body(Map("name" -> name))
      .post(uri"$applicationsUrl/orgs")
      .response(asJson[CloudFoundryOrg])

    implicit val backend = AkkaHttpBackend()
    val response: Future[Response[CloudFoundryOrg]] = request.send()

    response
  }
}
