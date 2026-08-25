resource "kubernetes_namespace_v1" "flink_disk_operator" {
  metadata {
    name = local.namespace_operator
  }

  depends_on = [module.k3d_cluster]
}

resource "kubernetes_service_account_v1" "flink_disk_operator" {
  metadata {
    name      = "flink-disk-operator"
    namespace = kubernetes_namespace_v1.flink_disk_operator.metadata[0].name
  }
}

resource "kubernetes_cluster_role_v1" "flink_disk_operator" {
  metadata {
    name = "flink-disk-operator"
  }

  rule {
    api_groups = ["flink.apache.org"]
    resources  = ["flinkdeployments"]
    verbs      = ["get", "list", "watch", "patch", "update"]
  }

  rule {
    api_groups = [""]
    resources  = ["configmaps"]
    verbs      = ["get", "list", "watch", "create", "update", "patch", "delete"]
  }

  rule {
    api_groups = ["apiextensions.k8s.io"]
    resources  = ["customresourcedefinitions"]
    verbs      = ["get", "list"]
  }
}

resource "kubernetes_cluster_role_binding_v1" "flink_disk_operator" {
  metadata {
    name = "flink-disk-operator"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role_v1.flink_disk_operator.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account_v1.flink_disk_operator.metadata[0].name
    namespace = kubernetes_namespace_v1.flink_disk_operator.metadata[0].name
  }
}

resource "kubernetes_deployment_v1" "flink_disk_operator" {
  metadata {
    name      = "flink-disk-operator"
    namespace = kubernetes_namespace_v1.flink_disk_operator.metadata[0].name
    labels = {
      app = "flink-disk-operator"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "flink-disk-operator"
      }
    }

    template {
      metadata {
        labels = {
          app = "flink-disk-operator"
        }
      }

      spec {
        service_account_name = kubernetes_service_account_v1.flink_disk_operator.metadata[0].name

        container {
          name              = "operator"
          image             = "${local.image_registry_in_cluster}/${local.flink_disk_operator_image}:${local.image_tag}"
          image_pull_policy = "Always"

          port {
            name           = "health"
            container_port = 8080
          }

          env {
            name  = "PROMETHEUS_URL"
            value = "http://mock-prometheus.${local.namespace_monitoring}.svc.cluster.local:9090"
          }

          env {
            name  = "DISK_THRESHOLD"
            value = local.flink_disk_threshold
          }

          env {
            name  = "DISK_TARGET_FILL"
            value = local.flink_disk_target_fill
          }

          # Never propose less than the size the chart ships with.
          env {
            name  = "DISK_MIN_SIZE"
            value = local.flink_local_storage_size
          }

          env {
            name  = "DISK_MAX_SIZE"
            value = local.flink_disk_max_size
          }

          env {
            name  = "DISK_SIZE_GRANULARITY"
            value = local.flink_disk_granularity
          }

          env {
            name  = "POLL_INTERVAL_SECONDS"
            value = local.flink_disk_poll_seconds
          }

          env {
            name  = "WATCH_NAMESPACES"
            value = local.namespace_applications
          }

          env {
            name  = "LOG_LEVEL"
            value = local.flink_disk_log_level
          }

          liveness_probe {
            http_get {
              path = "/healthz"
              port = "health"
            }
            initial_delay_seconds = 15
            period_seconds        = 20
          }

          readiness_probe {
            http_get {
              path = "/readyz"
              port = "health"
            }
            initial_delay_seconds = 5
            period_seconds        = 10
          }

          resources {
            requests = {
              cpu    = "100m"
              memory = "256Mi"
            }
            limits = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }
        }
      }
    }
  }

  depends_on = [
    helm_release.flink_operator,
    kubernetes_deployment_v1.mock_prometheus,
    kubernetes_cluster_role_binding_v1.flink_disk_operator,
  ]
}

output "flink_disk_operator_state_configmap" {
  description = "Where flink-disk-operator records the desired volume size for the demo job."
  value       = "${local.namespace_applications}/flink-disk-state-${local.flink_job_name}"
}
