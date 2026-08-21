resource "helm_release" "cert-manager" {
  name       = "cert-manager"
  repository = "https://charts.jetstack.io"
  chart      = "cert-manager"
  verify     = false

  set = [{
    name  = "installCRDs"
    value = "true"
  }]

  depends_on = [
    module.k3d_cluster
  ]
}
