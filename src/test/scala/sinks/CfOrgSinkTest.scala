package sinks

import org.apache.flink.apis.CfApplications
import org.apache.flink.cf.CloudFoundryApp
import org.apache.flink.sinks.CfOrgSink
import org.junit.Assert.assertEquals
import org.junit.Test
import org.scalamock.scalatest.MockFactory
import org.scalatest.FlatSpec

class CfOrgSinkTest extends FlatSpec with MockFactory {

  @Test
  def `should create a new org, space and app`(): Unit = {
    val cfApplicationsMock = stub[CfApplications]

    (cfApplicationsMock.createOrg _).when("testOrg").returns("{\"id\": \"org1\", \"name\": \"testOrg\"}")
    (cfApplicationsMock.createSpace _).when("org1", "testSpace").returns("{\"id\": \"space1\", \"orgId\": \"org1\", \"name\": \"testSpace\"}")
    (cfApplicationsMock.createApp _).when("space1", "appId1", "testApp").returns("{\"id\": \"id1\", \"spaceId\": \"space1\", \"appId\": \"appId1\", \"name\": \"testApp\"}")

    val cfApp: CloudFoundryApp = new CfOrgSink(cfApplicationsMock).createData("testOrg", "testSpace", "testApp", "appId1")

    assertEquals(cfApp.spaceId, "space1")
    assertEquals(cfApp.appId, "appId1")
    assertEquals(cfApp.name, "testApp")
  }
}
