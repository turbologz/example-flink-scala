package org.apache.flink.cf

@SerialVersionUID(100L)
case class CloudFoundrySpace(id: String, orgId: String, name: String) extends Serializable
