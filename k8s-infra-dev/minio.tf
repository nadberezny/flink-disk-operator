resource "helm_release" "minio" {
  name       = "minio"
  repository = "https://charts.bitnami.com/bitnami"
  chart      = "minio"
  version    = "12.13.0"

  namespace        = "cluster-services"
  create_namespace = true

  depends_on = [
    module.k3d_cluster
  ]

  set = [
    {
      name  = "auth.rootUser"
      value = local.minio_user
    },
    {
      name  = "auth.rootPassword"
      value = local.minio_password
    },
    {
      name  = "defaultBuckets"
      value = "${local.flink_ha_dir} ${local.flink_checkpoints_dir} ${local.flink_savepoints_dir}"
    },
    {
      name  = "volumePermissions.enabled"
      value = "true"
    },
    {
      name  = "image.repository"
      value = "bitnamilegacy/minio"
    },
    {
      name  = "clientImage.repository"
      value = "bitnamilegacy/minio-client"
    },
    {
      name  = "volumePermissions.image.repository"
      value = "bitnamilegacy/os-shell"
    }
  ]
}
