package org.apache.flink.kubernetes

import com.fasterxml.jackson.annotation.JsonIgnore

class KubernetesLogKubernetes(val container_name: String,
                              val namespace_name: String,
                              val pod_name: String,
                              val pod_id: String,
                              @JsonIgnore val labels: Any,
                              val host: String,
                              master_url: String,
                              namespace_id: String)