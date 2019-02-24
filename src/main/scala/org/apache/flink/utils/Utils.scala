package org.apache.flink.utils

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

import scala.reflect.ClassTag

class Utils {

  def parseJson[T: ClassTag](data: String): T = {
    (new ObjectMapper() with ScalaObjectMapper)
      .registerModule(DefaultScalaModule)
      .readValue(data, classManifest[T].erasure)
      .asInstanceOf[T]
  }
}
