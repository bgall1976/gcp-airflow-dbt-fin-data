variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region for Cloud Run, Artifact Registry, etc."
  type        = string
  default     = "us-east1"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "github_repo" {
  description = "GitHub repository in format owner/repo"
  type        = string
  default     = "your-org/financial-data-platform"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}
