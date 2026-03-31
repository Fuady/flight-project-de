#!/usr/bin/env bash
# scripts/setup.sh
# ─────────────────────────────────────────────────────────────────────────────
# One-time local setup script for the Flights DE project.
# Run: bash scripts/setup.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }

# ── 1. Check prerequisites ────────────────────────────────────────────────────
info "Checking prerequisites..."
for cmd in python3 docker docker-compose terraform gcloud; do
  if ! command -v $cmd &>/dev/null; then
    warn "$cmd not found. Please install it before continuing."
  else
    info "  ✓ $cmd found"
  fi
done

# ── 2. Python virtualenv ──────────────────────────────────────────────────────
info "Creating Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
info "  ✓ Python environment ready (.venv)"

# ── 3. Create directories ─────────────────────────────────────────────────────
mkdir -p keys airflow/logs
info "  ✓ Directories created"

# ── 4. Copy env template ──────────────────────────────────────────────────────
if [ ! -f .env ]; then
  cp .env.example .env
  warn ".env created from template. Edit it before running docker-compose."
fi

# ── 5. Copy terraform vars ────────────────────────────────────────────────────
if [ ! -f terraform/terraform.tfvars ]; then
  cp terraform/terraform.tfvars.example terraform/terraform.tfvars
  warn "terraform/terraform.tfvars created. Fill in your project_id."
fi

# ── 6. Airflow UID fix (Linux) ────────────────────────────────────────────────
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  echo "AIRFLOW_UID=$(id -u)" >> .env
  info "  ✓ AIRFLOW_UID set for Linux"
fi

echo ""
info "Setup complete! Next steps:"
echo "  1. Add your GCP service account key to: keys/sa-key.json"
echo "  2. Edit .env with your GCP_PROJECT_ID and bucket names"
echo "  3. Edit terraform/terraform.tfvars with your project_id"
echo "  4. Run Terraform:   cd terraform && terraform init && terraform apply"
echo "  5. Start services:  docker-compose up -d"
echo "  6. Open Airflow:    http://localhost:8080  (admin/admin)"
echo "  7. Open Dashboard:  http://localhost:8501"
