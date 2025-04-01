terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  required_version = "~>1.2"
}

provider "google" {
  project = var.common_params.spanner_project_id
  region  = var.common_params.region
}
