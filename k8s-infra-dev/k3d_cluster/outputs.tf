output "host" {
  value = resource.k3d_cluster.flink.host
}

output "client_certificate" {
  value = k3d_cluster.flink.client_certificate
  sensitive = true
}

output "client_key" {
  value = resource.k3d_cluster.flink.client_key
  sensitive = true
}

output "cluster_ca_certificate" {
  value = resource.k3d_cluster.flink.cluster_ca_certificate
  sensitive = true
}
