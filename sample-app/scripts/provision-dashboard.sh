#!/bin/bash
set -euo pipefail

# Provision the Grafana dashboard into the Amazon Managed Grafana workspace
# Requires: CDK outputs (workspace ID), AWS CLI

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infrastructure"
DASHBOARD_DIR="$SCRIPT_DIR/../grafana"

echo "=== KCL OTel Sample - Grafana Dashboard Provisioning ==="

# Read workspace ID from CDK outputs
if [ -f "$INFRA_DIR/outputs.json" ]; then
    WORKSPACE_ID=$(python3 -c "import json; d=json.load(open('$INFRA_DIR/outputs.json')); print(d['KclOtelSampleStack']['GrafanaWorkspaceId'])" 2>/dev/null)
    GRAFANA_URL=$(python3 -c "import json; d=json.load(open('$INFRA_DIR/outputs.json')); print(d['KclOtelSampleStack']['GrafanaWorkspaceUrl'])" 2>/dev/null)
else
    echo "ERROR: outputs.json not found. Run deploy.sh first."
    exit 1
fi

if [ -z "$WORKSPACE_ID" ]; then
    echo "ERROR: Could not read GrafanaWorkspaceId from outputs.json"
    exit 1
fi

echo "Workspace ID: $WORKSPACE_ID"
echo "Workspace URL: https://$GRAFANA_URL"
echo ""

# Create API key
echo "1. Creating Grafana API key..."
API_KEY_RESPONSE=$(aws grafana create-workspace-api-key \
    --workspace-id "$WORKSPACE_ID" \
    --key-name "dashboard-provisioner-$(date +%s)" \
    --key-role ADMIN \
    --seconds-to-live 300)

API_KEY=$(echo "$API_KEY_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['key'])")

if [ -z "$API_KEY" ]; then
    echo "ERROR: Failed to create API key. Ensure you have admin access to the workspace."
    exit 1
fi

GRAFANA_ENDPOINT="https://$GRAFANA_URL"

# Configure CloudWatch data source
echo "2. Configuring CloudWatch data source..."
REGION="${AWS_REGION:-us-west-2}"

curl -s -X POST "$GRAFANA_ENDPOINT/api/datasources" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
        \"name\": \"CloudWatch\",
        \"type\": \"cloudwatch\",
        \"access\": \"proxy\",
        \"jsonData\": {
            \"authType\": \"default\",
            \"defaultRegion\": \"$REGION\"
        }
    }" > /dev/null

echo "   CloudWatch data source configured"

# Import dashboard
echo "3. Importing KCL OTel metrics dashboard..."
DASHBOARD_JSON=$(cat "$DASHBOARD_DIR/kcl-otel-dashboard.json")

curl -s -X POST "$GRAFANA_ENDPOINT/api/dashboards/db" \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d "{
        \"dashboard\": $DASHBOARD_JSON,
        \"overwrite\": true,
        \"folderId\": 0
    }" > /dev/null

echo "   Dashboard imported successfully"
echo ""
echo "=== Done ==="
echo "Open Grafana: https://$GRAFANA_URL"
echo "Dashboard: https://$GRAFANA_URL/d/kcl-otel-metrics/kcl-opentelemetry-metrics"
