#!/bin/bash
set -e

# Change to project root directory (parent of scripts directory)
cd "$(dirname "$0")/.."

echo "🧪 Electron Microscopy Test Suite with Real Data"
echo "==============================================="

# Configuration
TEST_TYPE="${1:-integration}"
DATA_DIR="${EM_DATA_DIR:-./data}"
SERVICES=(${EM_SERVICES:-"ebi epfl flyem idr openorganelle"})

echo "🧪 Test type: $TEST_TYPE"
echo "💾 Data directory: $DATA_DIR"
echo "🔧 Services: ${SERVICES[*]}"

# Create data directories
echo "📁 Setting up data directories..."
mkdir -p "$DATA_DIR"/{ebi,epfl,flyem,idr,openorganelle}

# Function to run ingestion if data doesn't exist
check_and_ingest() {
    local service=$1
    local data_path="$DATA_DIR/$service"
    
    if [ ! "$(ls -A $data_path 2>/dev/null)" ]; then
        echo "📥 No data found for $service, running ingestion..."
        if ! ./run.sh; then
            echo "❌ Ingestion failed for $service"
            return 1
        fi
    else
        echo "✅ Data already exists for $service"
    fi
}

# Check if we need to run ingestion first
if [ "$TEST_TYPE" = "integration" ] || [ "$TEST_TYPE" = "with-data" ]; then
    echo "🔍 Checking for existing ingested data..."
    
    # Check each service for existing data
    need_ingestion=false
    for service in "${SERVICES[@]}"; do
        if [ ! "$(ls -A $DATA_DIR/$service 2>/dev/null)" ]; then
            need_ingestion=true
            break
        fi
    done
    
    if [ "$need_ingestion" = true ]; then
        echo "📥 Running data ingestion first..."
        if ! ./run.sh; then
            echo "❌ Data ingestion failed!"
            exit 1
        fi
    else
        echo "✅ Using existing ingested data"
    fi
fi

# Now run tests with the real data
echo "🏃 Running tests..."
if [ "$TEST_TYPE" = "with-data" ]; then
    # Use real data for testing instead of mocks
    EM_USE_REAL_DATA=true python run_tests.py integration
else
    python run_tests.py "$TEST_TYPE"
fi

echo "📊 Test completed! Data available in: $DATA_DIR"