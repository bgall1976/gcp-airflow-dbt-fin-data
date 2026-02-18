terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    # Override with: terraform init -backend-config="bucket=YOUR_BUCKET"
    bucket = "REPLACE_WITH_YOUR_TF_STATE_BUCKET"
    prefix = "financial-data-platform"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ================================================================
# ENABLE REQUIRED APIS
# ================================================================
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "sts.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

# ================================================================
# BIGQUERY DATASETS
# ================================================================
locals {
  raw_datasets = {
    raw_quickbooks = { source = "quickbooks" }
    raw_stripe     = { source = "stripe" }
    raw_netsuite   = { source = "netsuite" }
    raw_plaid      = { source = "plaid" }
    raw_salesforce = { source = "salesforce" }
  }

  dbt_datasets = {
    staging      = { layer = "staging" }
    intermediate = { layer = "intermediate" }
    marts        = { layer = "marts" }
    data_quality = { layer = "data_quality" }
    snapshots    = { layer = "snapshots" }
    seeds        = { layer = "seeds" }
  }
}

resource "google_bigquery_dataset" "raw" {
  for_each      = local.raw_datasets
  dataset_id    = each.key
  friendly_name = "Raw ${each.value.source} data"
  location      = var.location

  labels = {
    layer       = "raw"
    source      = each.value.source
    environment = var.environment
    managed_by  = "terraform"
  }

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

resource "google_bigquery_dataset" "dbt" {
  for_each      = local.dbt_datasets
  dataset_id    = each.key
  friendly_name = "${each.value.layer} layer"
  location      = var.location

  labels = {
    layer       = each.value.layer
    managed_by  = "dbt"
    environment = var.environment
  }

  depends_on = [google_project_service.apis["bigquery.googleapis.com"]]
}

# ================================================================
# SERVICE ACCOUNTS
# ================================================================

# Pipeline service account (Cloud Run Jobs)
resource "google_service_account" "pipeline" {
  account_id   = "fdp-pipeline"
  display_name = "Financial Data Pipeline"
  description  = "Runs extractors and dbt in Cloud Run Jobs"
}

resource "google_project_iam_member" "pipeline_roles" {
  for_each = toset([
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/bigquery.dataViewer",
    "roles/secretmanager.secretAccessor",
    "roles/run.invoker",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# GitHub Actions service account (WIF)
resource "google_service_account" "github_actions" {
  account_id   = "fdp-github-actions"
  display_name = "GitHub Actions (Financial Data Platform)"
  description  = "Used by GitHub Actions via Workload Identity Federation"
}

resource "google_project_iam_member" "github_actions_roles" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/bigquery.jobUser",
    "roles/run.admin",
    "roles/artifactregistry.writer",
    "roles/iam.serviceAccountUser",
    "roles/storage.admin",
    "roles/secretmanager.admin",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.github_actions.email}"
}

# ================================================================
# WORKLOAD IDENTITY FEDERATION (GitHub Actions -> GCP)
# ================================================================
resource "google_iam_workload_identity_pool" "github" {
  workload_identity_pool_id = "github-pool"
  display_name              = "GitHub Actions Pool"

  depends_on = [google_project_service.apis["iam.googleapis.com"]]
}

resource "google_iam_workload_identity_pool_provider" "github" {
  workload_identity_pool_id          = google_iam_workload_identity_pool.github.workload_identity_pool_id
  workload_identity_pool_provider_id = "github-provider"
  display_name                       = "GitHub OIDC"

  attribute_mapping = {
    "google.subject"       = "assertion.sub"
    "attribute.actor"      = "assertion.actor"
    "attribute.repository" = "assertion.repository"
  }

  attribute_condition = "assertion.repository == '${var.github_repo}'"

  oidc {
    issuer_uri = "https://token.actions.githubusercontent.com"
  }
}

resource "google_service_account_iam_member" "wif_binding" {
  service_account_id = google_service_account.github_actions.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${google_iam_workload_identity_pool.github.name}/attribute.repository/${var.github_repo}"
}

# ================================================================
# ARTIFACT REGISTRY
# ================================================================
resource "google_artifact_registry_repository" "images" {
  location      = var.region
  repository_id = "financial-data-platform"
  format        = "DOCKER"

  cleanup_policies {
    id     = "keep-last-10"
    action = "KEEP"
    most_recent_versions {
      keep_count = 10
    }
  }

  depends_on = [google_project_service.apis["artifactregistry.googleapis.com"]]
}

# ================================================================
# SECRET MANAGER
# ================================================================
locals {
  secrets = [
    "QB_CLIENT_ID", "QB_CLIENT_SECRET", "QB_REFRESH_TOKEN", "QB_REALM_ID",
    "STRIPE_API_KEY",
    "NS_ACCOUNT_ID", "NS_CONSUMER_KEY", "NS_CONSUMER_SECRET", "NS_TOKEN_ID", "NS_TOKEN_SECRET",
    "PLAID_CLIENT_ID", "PLAID_SECRET", "PLAID_ACCESS_TOKENS",
    "SF_USERNAME", "SF_PASSWORD", "SF_SECURITY_TOKEN",
  ]
}

resource "google_secret_manager_secret" "pipeline_secrets" {
  for_each  = toset(local.secrets)
  secret_id = each.value

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis["secretmanager.googleapis.com"]]
}

# ================================================================
# CLOUD SCHEDULER (daily triggers)
# ================================================================
locals {
  extractor_jobs = {
    quickbooks = { schedule = "0 6 * * *", timeout = "1800s" }
    stripe     = { schedule = "0 6 * * *", timeout = "1800s" }
    netsuite   = { schedule = "5 6 * * *", timeout = "3600s" }
    plaid      = { schedule = "0 6 * * *", timeout = "1800s" }
    salesforce = { schedule = "0 6 * * *", timeout = "1800s" }
  }
}

resource "google_cloud_scheduler_job" "extractors" {
  for_each = local.extractor_jobs

  name             = "fdp-extract-${each.key}"
  description      = "Daily ${each.key} extraction"
  schedule         = each.value.schedule
  time_zone        = "America/New_York"
  attempt_deadline = each.value.timeout

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/extract-${each.key}:run"
    oauth_token {
      service_account_email = google_service_account.pipeline.email
    }
  }

  depends_on = [google_project_service.apis["cloudscheduler.googleapis.com"]]
}

# GCS bucket for dbt docs
resource "google_storage_bucket" "dbt_docs" {
  name     = "${var.project_id}-dbt-docs"
  location = var.location

  website {
    main_page_suffix = "index.html"
  }

  uniform_bucket_level_access = true
}

# ================================================================
# OUTPUTS
# ================================================================
output "wif_provider" {
  description = "Set as GitHub secret: WIF_PROVIDER"
  value       = google_iam_workload_identity_pool_provider.github.name
}

output "wif_service_account" {
  description = "Set as GitHub secret: WIF_SERVICE_ACCOUNT"
  value       = google_service_account.github_actions.email
}

output "pipeline_service_account" {
  description = "Set as GitHub secret: PIPELINE_SERVICE_ACCOUNT"
  value       = google_service_account.pipeline.email
}

output "artifact_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.images.repository_id}"
}

output "dbt_docs_url" {
  value = "https://storage.googleapis.com/${google_storage_bucket.dbt_docs.name}/index.html"
}

output "datasets" {
  value = merge(
    { for k, v in google_bigquery_dataset.raw : k => v.dataset_id },
    { for k, v in google_bigquery_dataset.dbt : k => v.dataset_id },
  )
}
