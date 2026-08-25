resource "helm_release" "flink_operator" {
  name             = "flink-operator"
  repository       = "https://downloads.apache.org/flink/flink-kubernetes-operator-${local.flink_operator_version}/"
  chart            = "flink-kubernetes-operator"
  namespace        = local.namespace_applications
  create_namespace = true

  values = [yamlencode({
    image = {
      repository = "${local.image_registry_in_cluster}/${local.flink_operator_image}"
      tag        = local.image_tag
      pullPolicy = "Always"
    }

    defaultConfiguration = {
      create = true
      append = true

      "flink-conf.yaml" = join("\n", [
        "# Flink Config Overrides",
        "kubernetes.operator.metrics.reporter.slf4j.factory.class: org.apache.flink.metrics.slf4j.Slf4jReporterFactory",
        "kubernetes.operator.metrics.reporter.slf4j.interval: 5 MINUTE",
        "",
        "kubernetes.operator.reconcile.interval: 15 s",
        "kubernetes.operator.observer.progress-check.interval: 5 s",
        "",
        file("${path.module}/../mutator/flink-conf-overrides.yaml"),
      ])
    }
  })]

  depends_on = [
    module.k3d_cluster, helm_release.cert-manager
  ]

  timeout = 1500
}
