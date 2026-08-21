resource "helm_release" "flink_job" {
  name             = local.flink_job_name
  chart            = "../k8s-helm/flink-disk-job"
  namespace        = local.namespace_applications
  create_namespace = true

  timeout = 900

  set = [
    {
      name  = "name"
      value = local.flink_job_name
    },
    {
      name  = "namespace"
      value = local.namespace_applications
    },
    {
      name  = "jobName"
      value = local.flink_job_name
    },
    {
      name  = "image.repository"
      value = "${local.image_registry_in_cluster}/${local.flink_job_image}"
    },
    {
      name  = "image.tag"
      value = local.image_tag
    },
    {
      name  = "localStorage.volumeName"
      value = local.flink_local_storage_volume
    },
    {
      name  = "localStorage.size"
      value = local.flink_local_storage_size
    },
    {
      name  = "localStorage.storageClassName"
      value = "local-path"
    },
  ]

  depends_on = [
    helm_release.flink_operator,
  ]
}

output "flink_job_taskmanager_pvc_hint" {
  description = "PVC name pattern the operator/mock use for this job's TaskManager volumes."
  value       = "${local.flink_job_name}-taskmanager-<attempt>-<index>-${local.flink_local_storage_volume}"
}
