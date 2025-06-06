#!/bin/bash
set -e

echo "🚀 Electron Microscopy Data Ingestion Pipeline"
echo "=============================================="

# Configuration
CONFIG_FILE="${EM_CONFIG_FILE:-./config/config.yaml}"
DATA_DIR="${EM_DATA_DIR:-./data}"
LOGS_DIR="${EM_LOGS_DIR:-./logs}"

# Create necessary directories
echo "📁 Setting up directories..."
mkdir -p "$DATA_DIR"/{ebi,epfl,flyem,idr,openorganelle}
mkdir -p "$LOGS_DIR"

# Validate configuration exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Configuration file not found: $CONFIG_FILE"
    echo "Please create configuration file or set EM_CONFIG_FILE environment variable"
    exit 1
fi

echo "📋 Using configuration: $CONFIG_FILE"
echo "💾 Data will be stored in: $DATA_DIR"
echo "📝 Logs will be stored in: $LOGS_DIR"

# Services to run (can be overridden by environment variable)
if [ -n "$EM_SERVICES" ]; then
    SERVICES=($EM_SERVICES)
else
    SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")
fi

echo "🔧 Services to run: ${SERVICES[*]}"

# Function to run a single service
run_service() {
    local service=$1
    echo "=== Starting $service ingestion ==="
    
    # Set service-specific environment variables if they exist
    case $service in
        "ebi")
            export EBI_ENTRY_ID="${EBI_ENTRY_ID:-11759}"
            ;;
        "flyem")
            export FLYEM_INSTANCE="${FLYEM_INSTANCE:-grayscale}"
            export FLYEM_CROP_SIZE="${FLYEM_CROP_SIZE:-1000,1000,1000}"
            ;;
        "idr")
            export IDR_IMAGE_IDS="${IDR_IMAGE_IDS:-9846137}"
            export IDR_OUTPUT_DIR="$DATA_DIR/idr"
            ;;
    esac
    
    # Run the service
    if docker compose run --rm "$service" > "$LOGS_DIR/${service}.log" 2>&1; then
        echo "✅ $service completed successfully"
    else
        echo "❌ $service failed (check $LOGS_DIR/${service}.log)"
        return 1
    fi
}

# Option to run services sequentially or in parallel
if [ "${EM_SEQUENTIAL:-false}" = "true" ]; then
    echo "🔄 Running services sequentially..."
    failed_services=()
    
    for service in "${SERVICES[@]}"; do
        if ! run_service "$service"; then
            failed_services+=("$service")
        fi
    done
    
    if [ ${#failed_services[@]} -gt 0 ]; then
        echo "❌ Failed services: ${failed_services[*]}"
        exit 1
    fi
else
    echo "⚡ Running services in parallel..."
    pids=()
    
    # Launch each ingestion container in the background
    for service in "${SERVICES[@]}"; do
        run_service "$service" &
        pids+=($!)
    done
    
    # Wait for all background jobs and check their exit status
    failed=0
    for i in "${!pids[@]}"; do
        if ! wait "${pids[$i]}"; then
            echo "❌ Service ${SERVICES[$i]} failed"
            failed=1
        fi
    done
    
    if [ $failed -eq 1 ]; then
        echo "❌ Some services failed. Check logs in $LOGS_DIR"
        exit 1
    fi
fi

echo ""
echo "✅ All ingestion jobs completed successfully!"

# Optionally run consolidation
if [ "${EM_RUN_CONSOLIDATION:-true}" = "true" ]; then
    echo ""
    echo "📊 Running metadata consolidation..."
    
    if docker compose run --rm consolidate > "$LOGS_DIR/consolidate.log" 2>&1; then
        echo "✅ Metadata consolidation completed"
        echo "📄 Results available in app/consolidate/"
    else
        echo "❌ Metadata consolidation failed (check $LOGS_DIR/consolidate.log)"
        exit 1
    fi
fi

echo ""
echo "🎉 Pipeline execution complete!"
echo "📊 Data stored in: $DATA_DIR"
echo "📋 Logs available in: $LOGS_DIR"
echo "📈 Metadata catalog in: app/consolidate/"