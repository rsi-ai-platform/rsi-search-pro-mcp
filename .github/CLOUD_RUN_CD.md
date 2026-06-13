# Continuous Deployment to Cloud Run

GitHub Actions builds + deploys on every push to `main`. Auth is via
Workload Identity Federation (no long-lived service-account key).

## One-time GCP setup

The WIF pool's attribute condition already accepts every
`rsi-ai-platform/*` repo (widened earlier so we don't have to update GCP
when a new repo joins). The only per-service step is creating a deployer
SA.

```bash
PROJECT=silverfox-454313
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT" --format='value(projectNumber)')
SERVICE=rsi-search-pro-mcp
SA="${SERVICE}-deployer"
GITHUB_REPO="rsi-ai-platform/${SERVICE}"

# 1. Create the deployer SA
gcloud iam service-accounts create "$SA" \
  --project="$PROJECT" \
  --display-name="GitHub Actions → Cloud Run deployer for ${SERVICE}"

SA_EMAIL="${SA}@${PROJECT}.iam.gserviceaccount.com"

# 2. Grant build + push + deploy + impersonate runtime SA
for ROLE in roles/run.admin roles/artifactregistry.writer roles/iam.serviceAccountUser; do
  gcloud projects add-iam-policy-binding "$PROJECT" \
    --member="serviceAccount:$SA_EMAIL" --role="$ROLE" --condition=None
done

# 3. Bind GitHub repo to the SA via the existing WIF pool
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-actions/attribute.repository/${GITHUB_REPO}"
```

## GitHub secrets

```
WIF_PROVIDER   projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/github-actions/providers/github-provider
WIF_SA         rsi-search-pro-mcp-deployer@silverfox-454313.iam.gserviceaccount.com
```

Set via:

```bash
gh secret set WIF_PROVIDER --repo rsi-ai-platform/rsi-search-pro-mcp \
  --body "projects/<NUM>/locations/global/workloadIdentityPools/github-actions/providers/github-provider"
gh secret set WIF_SA --repo rsi-ai-platform/rsi-search-pro-mcp \
  --body "rsi-search-pro-mcp-deployer@silverfox-454313.iam.gserviceaccount.com"
```

## Manual deploy fallback

```bash
SHA=$(git rev-parse --short HEAD)
docker buildx build --platform linux/amd64 \
  -t asia-south1-docker.pkg.dev/silverfox-454313/agentic-rag/rsi-search-pro-mcp:latest \
  -t asia-south1-docker.pkg.dev/silverfox-454313/agentic-rag/rsi-search-pro-mcp:$SHA \
  --push .
gcloud run deploy rsi-search-pro-mcp \
  --image=asia-south1-docker.pkg.dev/silverfox-454313/agentic-rag/rsi-search-pro-mcp:$SHA \
  --region=asia-south1 --project=silverfox-454313 --quiet
```
