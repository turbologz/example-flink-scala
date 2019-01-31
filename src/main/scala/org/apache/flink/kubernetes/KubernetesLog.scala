package org.apache.flink.kubernetes

class KubernetesLog(val log: String,
                    val stream: String,
                    val docker: KubernetesLogDocker,
                    val kubernetes: KubernetesLogKubernetes,
                    val time: Number,
                    val tag: String)