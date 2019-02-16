package org.apache.flink.cf

class CloudFoundryLog(val host: String,
                      val ident: String,
                      val message: String,
                      val time: Number,
                      val pid: String,
                      val tag: String,
                      val extradata: String,
                      val msgid: String)