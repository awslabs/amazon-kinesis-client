#!/bin/bash
set -euo pipefail

# Run the data producer to populate the Kinesis stream with sample records
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="$SCRIPT_DIR/../app"

echo "=== KCL OTel Sample - Data Producer ==="

# Build the app if not already built
if [ ! -f "$APP_DIR/target/kcl-otel-sample-app-1.0.0.jar" ]; then
    echo "Building sample app..."
    cd "$APP_DIR"
    mvn package -DskipTests -q
fi

# Run producer
echo "Producing records to stream: ${STREAM_NAME:-kcl-otel-sample-stream}"
echo "Rate: ${PRODUCER_RECORDS_PER_SECOND:-10}/s, Duration: ${PRODUCER_DURATION_SECONDS:-300}s"
echo ""

cd "$APP_DIR"
java -cp target/kcl-otel-sample-app-1.0.0.jar \
    software.amazon.kinesis.sample.otel.SampleDataProducer
