output "setup_instructions" {
  description = "Post-apply instructions"
  value       = <<-EOT

    ============================================================
    SETUP COMPLETE - Next steps:
    ============================================================

    1. Set these GitHub repository SECRETS:
       WIF_PROVIDER           = ${google_iam_workload_identity_pool_provider.github.name}
       WIF_SERVICE_ACCOUNT    = ${google_service_account.github_actions.email}
       PIPELINE_SERVICE_ACCOUNT = ${google_service_account.pipeline.email}
       SLACK_WEBHOOK_URL      = (your Slack webhook, optional)

    2. Set these GitHub repository VARIABLES:
       GCP_PROJECT_ID  = ${var.project_id}
       GCP_REGION      = ${var.region}
       TF_STATE_BUCKET = (your terraform state bucket name)
       DBT_DOCS_BUCKET = ${google_storage_bucket.dbt_docs.name}

    3. Populate Secret Manager values:
       ${join("\n       ", [for s in local.secrets : "gcloud secrets versions add ${s} --data-file=<(echo 'YOUR_VALUE')"])}

    4. Push to main to trigger first deployment.
    ============================================================
  EOT
}
