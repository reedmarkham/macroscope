#!/bin/bash
set -e

# TO-DO: add "flyem" once that loader is working
SERVICES=("ebi" "epfl" "idr" "openorganelle")

mkdir -p logs

# Launch each ingestion container in the background
for SERVICE in "${SERVICES[@]}"; do
  echo "=== Starting $SERVICE ingestion ==="
  docker compose run --rm "$SERVICE" > "logs/${SERVICE}.log" 2>&1 &
done

# Wait for all background jobs to finish
wait

echo "✅ All ingestion jobs complete."