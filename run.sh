#!/bin/bash
set -e

SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")

mkdir -p logs

for SERVICE in "${SERVICES[@]}"; do
  echo "=== Starting $SERVICE ingestion ==="
  docker-compose run --rm "$SERVICE" > "logs/${SERVICE}.log" 2>&1
  echo "=== Finished $SERVICE. Logs saved to logs/${SERVICE}.log ==="
done

echo "✅ All ingestion jobs complete."
