package sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.cf.{CloudFoundryApp, CloudFoundryOrg, CloudFoundrySpace}
import org.apache.flink.sinks.CfOrgSink
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class CfOrgSinkTest extends FlatSpec with MockFactory {

  @Test
  def `should create a new org`(): Unit = {
    val cfApplicationsMock = stub[CfApplications]

    (cfApplicationsMock.createOrg _).when("testOrg").returns("{\"id\": \"id1\", \"name\": \"testOrg\"}")

    val cfOrg: CloudFoundryOrg = new CfOrgSink(cfApplicationsMock).createOrg("testOrg")

    assertEquals(cfOrg.name, "testOrg")
  }

  @Test
  def `should create a new space`(): Unit = {
    val cfApplicationsMock = stub[CfApplications]

    (cfApplicationsMock.createSpace _).when("org1", "testSpace").returns("{\"id\": \"id1\", \"orgId\": \"org1\", \"name\": \"testSpace\"}")

    val cfSpace: CloudFoundrySpace = new CfOrgSink(cfApplicationsMock).createSpace("org1", "testSpace")

    assertEquals(cfSpace.orgId, "org1")
    assertEquals(cfSpace.name, "testSpace")
  }

  @Test
  def `should create a new app`(): Unit = {
    val cfApplicationsMock = stub[CfApplications]

    (cfApplicationsMock.createApp _).when("space1", "appId1", "testApp").returns("{\"id\": \"id1\", \"spaceId\": \"space1\", \"appId\": \"appId1\", \"name\": \"testApp\"}")

    val cfApp: CloudFoundryApp = new CfOrgSink(cfApplicationsMock).createApp("space1", "appId1", "testApp")

    assertEquals(cfApp.spaceId, "space1")
    assertEquals(cfApp.appId, "appId1")
    assertEquals(cfApp.name, "testApp")
  }
}
