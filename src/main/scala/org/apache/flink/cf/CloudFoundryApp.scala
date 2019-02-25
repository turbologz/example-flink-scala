package org.apache.flink.cf

@SerialVersionUID(100L)
case class CloudFoundryApp(id: String, spaceId: String, appId: String, name: String) extends Serializable
