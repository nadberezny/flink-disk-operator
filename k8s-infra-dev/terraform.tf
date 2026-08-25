terraform {
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.36.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = ">= 1.7.0"
    }
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.17.0"
    }
    minio = {
      version = "1.13.0"
      source  = "aminueza/minio"
    }
  }
}
