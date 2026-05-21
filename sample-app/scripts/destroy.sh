#!/bin/bash
set -euo pipefail

# Tear down the KCL OTel Sample App infrastructure
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$SCRIPT_DIR/../infrastructure"

echo "=== KCL OTel Sample App - Destroy ==="
echo "This will delete ALL resources (stream, ECS service, Grafana workspace, etc.)"
read -p "Are you sure? (y/N) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd "$INFRA_DIR"
    cdk destroy KclOtelSampleStack --force
    echo "Stack destroyed."
else
    echo "Cancelled."
fi
