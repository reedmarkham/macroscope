#!/bin/bash

echo "Running legacy metadata fix script..."
echo "======================================"

# Run the fix script inside the consolidate container which has access to metadata_manager
docker compose run --rm --entrypoint="" consolidate python /app/scripts/fix_legacy_metadata.py

echo ""
echo "Metadata fix complete. Running validation to check results..."
echo "=============================================================="

# Run validation to see the results
docker compose run --rm consolidate

echo ""
echo "Fix and validation complete!"