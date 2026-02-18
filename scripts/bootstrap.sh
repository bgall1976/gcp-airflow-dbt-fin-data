#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Financial Data Platform - Bootstrap Script
#
# Provisions all GCP infrastructure and configures GitHub
# secrets for CI/CD deployment.
#
# Prerequisites:
#   - gcloud CLI authenticated with Owner/Editor role
#   - gh CLI authenticated (for setting GitHub secrets)
#   - terraform >= 1.5
# ============================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
err()  { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

# ---- Configuration ----
GCP_PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID environment variable}"
GCP_REGION="${GCP_REGION:-us-east1}"
GITHUB_REPO="${GITHUB_REPO:?Set GITHUB_REPO as owner/repo}"
TF_STATE_BUCKET="${TF_STATE_BUCKET:-${GCP_PROJECT_ID}-tf-state}"

log "Project:    $GCP_PROJECT_ID"
log "Region:     $GCP_REGION"
log "GitHub:     $GITHUB_REPO"
log "TF Bucket:  $TF_STATE_BUCKET"
echo ""

# ---- Step 1: Set active project ----
log "Setting active GCP project..."
gcloud config set project "$GCP_PROJECT_ID"

# ---- Step 2: Create Terraform state bucket ----
log "Creating Terraform state bucket..."
gsutil ls -b "gs://$TF_STATE_BUCKET" 2>/dev/null || \
  gsutil mb -l "$GCP_REGION" "gs://$TF_STATE_BUCKET"
gsutil versioning set on "gs://$TF_STATE_BUCKET"

# ---- Step 3: Run Terraform ----
log "Initializing Terraform..."
cd terraform

terraform init -backend-config="bucket=$TF_STATE_BUCKET"

log "Planning infrastructure..."
terraform plan \
  -var="project_id=$GCP_PROJECT_ID" \
  -var="region=$GCP_REGION" \
  -var="github_repo=$GITHUB_REPO" \
  -out=tfplan

read -rp "Apply Terraform plan? (y/N) " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
  err "Aborted."
fi

log "Applying infrastructure..."
terraform apply tfplan

# ---- Step 4: Capture Terraform outputs ----
WIF_PROVIDER=$(terraform output -raw wif_provider)
WIF_SA=$(terraform output -raw wif_service_account)
PIPELINE_SA=$(terraform output -raw pipeline_service_account)
DBT_DOCS_BUCKET="${GCP_PROJECT_ID}-dbt-docs"

cd ..

# ---- Step 5: Set GitHub secrets and variables ----
log "Configuring GitHub repository secrets..."

gh secret set WIF_PROVIDER           --repo "$GITHUB_REPO" --body "$WIF_PROVIDER"
gh secret set WIF_SERVICE_ACCOUNT    --repo "$GITHUB_REPO" --body "$WIF_SA"
gh secret set PIPELINE_SERVICE_ACCOUNT --repo "$GITHUB_REPO" --body "$PIPELINE_SA"

log "Configuring GitHub repository variables..."

gh variable set GCP_PROJECT_ID  --repo "$GITHUB_REPO" --body "$GCP_PROJECT_ID"
gh variable set GCP_REGION      --repo "$GITHUB_REPO" --body "$GCP_REGION"
gh variable set TF_STATE_BUCKET --repo "$GITHUB_REPO" --body "$TF_STATE_BUCKET"
gh variable set DBT_DOCS_BUCKET --repo "$GITHUB_REPO" --body "$DBT_DOCS_BUCKET"

# ---- Step 6: Remind about secrets ----
echo ""
log "============================================================"
log "Infrastructure deployed successfully!"
log "============================================================"
echo ""
warn "You still need to populate Secret Manager with API credentials:"
echo ""
echo "  gcloud secrets versions add QB_CLIENT_ID     --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add QB_CLIENT_SECRET  --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add QB_REFRESH_TOKEN  --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add QB_REALM_ID       --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add STRIPE_API_KEY    --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add NS_ACCOUNT_ID     --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add NS_CONSUMER_KEY   --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add NS_CONSUMER_SECRET --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add NS_TOKEN_ID       --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add NS_TOKEN_SECRET   --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add PLAID_CLIENT_ID   --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add PLAID_SECRET      --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add PLAID_ACCESS_TOKENS --data-file=<(echo 'token1,token2')"
echo "  gcloud secrets versions add SF_USERNAME       --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add SF_PASSWORD       --data-file=<(echo 'YOUR_VALUE')"
echo "  gcloud secrets versions add SF_SECURITY_TOKEN --data-file=<(echo 'YOUR_VALUE')"
echo ""
warn "Optional: Set SLACK_WEBHOOK_URL GitHub secret for failure notifications:"
echo "  gh secret set SLACK_WEBHOOK_URL --repo $GITHUB_REPO --body 'https://hooks.slack.com/...'"
echo ""
log "Push to main branch to trigger your first deployment!"
