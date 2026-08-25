resource "k3d_cluster" "flink" {
  name = "flink"

  # See https://k3d.io/v5.4.6/usage/configfile/#config-options
  k3d_config = <<EOF
apiVersion: k3d.io/v1alpha5
kind: Simple

# Expose ports 80 via 3080 and 443 via 8443.
ports:
  - port: 3080:80
    nodeFilters:
      - loadbalancer
  - port: 3443:443
    nodeFilters:
      - loadbalancer
volumes:
  - volume: /tmp/k3d:/var/lib/rancher/k3s/storage
    nodeFilters:
      - agent:*
      - server:*
registries:
  create:
    name: flink
    hostPort: "12345"
options:
  kubeconfig:
    updateDefaultKubeconfig: true
    switchCurrentContext: true
  k3s:
    extraArgs:
      # we are using ingress-nginx
      - arg: --disable=traefik
        nodeFilters:
          - server:*
EOF
}
