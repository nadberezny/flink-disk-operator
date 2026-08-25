resource "kubernetes_namespace_v1" "monitoring" {
  metadata {
    name = local.namespace_monitoring
  }

  depends_on = [module.k3d_cluster]
}

resource "kubernetes_deployment_v1" "mock_prometheus" {
  metadata {
    name      = "mock-prometheus"
    namespace = kubernetes_namespace_v1.monitoring.metadata[0].name
    labels = {
      app = "mock-prometheus"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "mock-prometheus"
      }
    }

    template {
      metadata {
        labels = {
          app = "mock-prometheus"
        }
        annotations = {
          "flink-disk-operator.nadberezny.com/seed-hash" = sha256(local.mock_prometheus_seed)
        }
      }

      spec {
        container {
          name              = "mock-prometheus"
          image             = "${local.image_registry_in_cluster}/${local.mock_prometheus_image}:${local.image_tag}"
          image_pull_policy = "IfNotPresent"

          port {
            name           = "http"
            container_port = 9090
          }

          env {
            name  = "PORT"
            value = "9090"
          }

          env {
            name  = "MOCK_SEED"
            value = local.mock_prometheus_seed
          }

          env {
            name  = "MOCK_AUTO_REGISTER"
            value = tostring(local.mock_prometheus_auto_register)
          }

          env {
            name  = "MOCK_AUTO_DEFAULTS"
            value = local.mock_prometheus_auto_defaults
          }

          readiness_probe {
            http_get {
              path = "/-/ready"
              port = "http"
            }
            initial_delay_seconds = 2
            period_seconds        = 5
          }

          liveness_probe {
            http_get {
              path = "/-/healthy"
              port = "http"
            }
            initial_delay_seconds = 10
            period_seconds        = 15
          }

          resources {
            requests = {
              cpu    = "50m"
              memory = "128Mi"
            }
            limits = {
              cpu    = "250m"
              memory = "256Mi"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service_v1" "mock_prometheus" {
  metadata {
    name      = "mock-prometheus"
    namespace = kubernetes_namespace_v1.monitoring.metadata[0].name
    labels = {
      app = "mock-prometheus"
    }
  }

  spec {
    selector = {
      app = "mock-prometheus"
    }

    port {
      name        = "http"
      port        = 9090
      target_port = "http"
    }
  }
}

resource "kubernetes_ingress_v1" "mock_prometheus" {
  metadata {
    name      = "mock-prometheus"
    namespace = kubernetes_namespace_v1.monitoring.metadata[0].name
  }

  spec {
    ingress_class_name = "nginx"

    rule {
      host = local.mock_prometheus_host

      http {
        path {
          path      = "/"
          path_type = "Prefix"

          backend {
            service {
              name = kubernetes_service_v1.mock_prometheus.metadata[0].name
              port {
                number = 9090
              }
            }
          }
        }
      }
    }
  }

  depends_on = [helm_release.nginx-ingress]
}

output "mock_prometheus_url_in_cluster" {
  description = "Base URL the flink-disk-operator should point its Prometheus client at."
  value       = "http://mock-prometheus.${local.namespace_monitoring}.svc.cluster.local:9090"
}

output "mock_prometheus_url_from_host" {
  description = "Base URL for poking the mock with curl from the laptop."
  value       = "http://${local.mock_prometheus_host}:3080"
}
