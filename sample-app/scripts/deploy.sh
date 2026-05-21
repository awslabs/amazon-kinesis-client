#!/bin/bash
set -euo pipefail

# Deploy the KCL OTel Sample App via CDK
# Prerequisites: AWS CLI configured, CDK CLI installed, Docker running

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infrastructure"

echo "=== KCL OTel Sample App - CDK Deployment ==="
echo ""

# Check prerequisites
command -v cdk >/dev/null 2>&1 || { echo "ERROR: AWS CDK CLI not found. Install with: npm install -g aws-cdk"; exit 1; }
command -v docker >/dev/null 2>&1 || { echo "ERROR: Docker not found."; exit 1; }
command -v mvn >/dev/null 2>&1 || { echo "ERROR: Maven not found."; exit 1; }

# Bootstrap CDK if needed
echo "1. Bootstrapping CDK (if needed)..."
cdk bootstrap 2>/dev/null || true

# Deploy
echo "2. Deploying CDK stack..."
cd "$INFRA_DIR"
cdk deploy KclOtelSampleStack --require-approval never --outputs-file outputs.json

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Stack outputs:"
cat outputs.json | python3 -m json.tool 2>/dev/null || cat outputs.json
echo ""
echo "Next steps:"
echo "  1. Run the data producer:  ./scripts/produce-data.sh"
echo "  2. Wait 2-3 minutes for metrics to appear in CloudWatch"
echo "  3. Import the Grafana dashboard: ./scripts/provision-dashboard.sh"
echo "  4. Open the Grafana workspace URL from the outputs above"
