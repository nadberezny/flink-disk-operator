provider "kubernetes" {
  host                   = module.k3d_cluster.host
  client_certificate     = base64decode(module.k3d_cluster.client_certificate)
  client_key             = base64decode(module.k3d_cluster.client_key)
  cluster_ca_certificate = base64decode(module.k3d_cluster.cluster_ca_certificate)
}

provider "helm" {
  kubernetes = {
    host                   = module.k3d_cluster.host
    client_certificate     = base64decode(module.k3d_cluster.client_certificate)
    client_key             = base64decode(module.k3d_cluster.client_key)
    cluster_ca_certificate = base64decode(module.k3d_cluster.cluster_ca_certificate)
  }
}

provider "kubectl" {
  host                   = module.k3d_cluster.host
  client_certificate     = base64decode(module.k3d_cluster.client_certificate)
  client_key             = base64decode(module.k3d_cluster.client_key)
  cluster_ca_certificate = base64decode(module.k3d_cluster.cluster_ca_certificate)
}
