locals {
  namespace_applications = "stream"
  namespace_services     = "cluster-services"
  namespace_monitoring   = "monitoring"
  namespace_operator     = "flink-disk-operator"

  flink_operator_version = "1.15.0"

  flink_ha_dir          = "high-availability/flink"
  flink_checkpoints_dir = "checkpoints/flink"
  flink_savepoints_dir  = "savepoints/flink"
  minio_user            = "admin"
  minio_password        = "adminadmin"

  image_registry_in_cluster = "nadberezny"
  flink_job_image           = "flink-noop-job"
  image_tag                 = "latest"
  mock_prometheus_image     = "mock-prometheus"
  flink_disk_operator_image = "flink-disk-operator"

  flink_operator_image = "flink-kubernetes-operator-disk"

  docker_platform = "linux/arm64"

  flink_job_name             = "flink-disk-job"
  flink_local_storage_volume = "local-storage"
  flink_local_storage_size   = "1Gi"

  mock_prometheus_seed = join(";", [
    join(",", [
      "namespace=${local.namespace_applications}",
      "pvc=${local.flink_job_name}-taskmanager-1-1-${local.flink_local_storage_volume}",
      "pod=${local.flink_job_name}-taskmanager-1-1",
      "capacity=${local.flink_local_storage_size}",
      "used=300Mi",
      "growth=5Mi",
    ])
  ])

  mock_prometheus_auto_register = true
  mock_prometheus_auto_defaults = "capacity=${local.flink_local_storage_size},used=850Mi,growth=1Mi"
  mock_prometheus_host          = "mock-prometheus.localtest.me"

  flink_disk_threshold    = "0.80"
  flink_disk_target_fill  = "0.50"
  flink_disk_max_size     = "8Gi"
  flink_disk_granularity  = "1Gi"
  flink_disk_poll_seconds = "15"
  flink_disk_log_level    = "debug"
}
