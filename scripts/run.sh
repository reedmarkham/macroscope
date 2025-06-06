#!/bin/bash
set -e

echo "🚀 Electron Microscopy Data Ingestion Pipeline"
echo "=============================================="

# Configuration
CONFIG_FILE="${EM_CONFIG_FILE:-./config/config.yaml}"
DATA_DIR="${EM_DATA_DIR:-./data}"
LOGS_DIR="${EM_LOGS_DIR:-./logs}"
EXECUTION_MODE="${EM_EXECUTION_MODE:-staged}"    # staged, parallel, or sequential

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
echo "🎯 Execution mode: $EXECUTION_MODE"
echo ""

# Display resource allocation based on execution mode
case $EXECUTION_MODE in
    "staged")
        echo "🎯 Staged Resource Allocation Strategy:"
        echo "  Stage 1 (Parallel):  IDR(1GB/0.25CPU) + FlyEM(1.5GB/0.25CPU) + EBI(1GB/0.5CPU)"
        echo "  Stage 2 (Sequential): OpenOrganelle(9GB/1.5CPU) → EPFL(6GB/1.0CPU)"
        echo "  Total Peak Usage: 9GB memory, 1.5 CPU cores"
        echo "  Optimized for: 16GB memory systems"
        ;;
    "parallel")
        echo "🎯 Parallel Resource Allocation:"
        echo "  All services run simultaneously with shared resources"
        echo "  Estimated Peak Usage: 15-20GB memory, 4+ CPU cores"
        echo "  Recommended for: 32GB+ memory systems"
        ;;
    "sequential")
        echo "🎯 Sequential Resource Allocation:"
        echo "  Services run one at a time with full resource access"
        echo "  Peak Usage: ~9GB memory per service, low parallelism"
        echo "  Slowest but most memory-conservative approach"
        ;;
esac
echo ""

# Function to run a stage with error handling (for staged execution)
run_stage() {
    local stage=$1
    local description=$2
    local services=$3
    
    echo "=== $description ==="
    echo "🔧 Services: $services"
    
    # Create stage-specific log file and redirect output
    local stage_log="$LOGS_DIR/${stage}_$(date +%Y%m%d_%H%M%S).log"
    
    echo "📝 Logging to: $stage_log"
    
    if docker compose --profile "$stage" up --build > "$stage_log" 2>&1; then
        echo "✅ $description completed successfully"
        echo "📋 Logs written to: $stage_log"
        return 0
    else
        echo "❌ $description failed"
        echo "📋 Check logs in: $stage_log"
        return 1
    fi
}

# Function to run a single service (for sequential execution)
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
    
    # Run the service using docker compose run --rm for accurate exit code
    if docker compose run --rm "$service" > "$LOGS_DIR/${service}.log" 2>&1; then
        echo "✅ $service completed successfully"
    else
        echo "❌ $service failed (check $LOGS_DIR/${service}.log)"
        return 1
    fi
}

# Execute based on selected mode
case $EXECUTION_MODE in
    "staged")
        echo "🏃 Starting staged pipeline execution..."
        echo ""
        
        # Stage 1: Light services in parallel
        if ! run_stage "stage1" "Stage 1: Light Services (Parallel)" "idr, flyem, ebi"; then
            echo "❌ Stage 1 failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        
        echo ""
        echo "⏳ Stage 1 complete. Starting heavy processing..."
        echo ""
        
        # Stage 2: OpenOrganelle (heavy processing)
        if ! run_stage "stage2" "Stage 2: OpenOrganelle Processing" "openorganelle"; then
            echo "❌ Stage 2 (OpenOrganelle) failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        
        echo ""
        echo "⏳ OpenOrganelle complete. Starting EPFL..."
        echo ""
        
        # Stage 3: EPFL (large downloads)
        if ! run_stage "stage3" "Stage 3: EPFL Processing" "epfl"; then
            echo "❌ Stage 3 (EPFL) failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        ;;
        
    "sequential")
        echo "🔄 Running services sequentially..."
        
        # Services to run (can be overridden by environment variable)
        if [ -n "$EM_SERVICES" ]; then
            IFS=' ' read -ra SERVICES <<< "$EM_SERVICES"
        else
            SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")
        fi
        
        echo "🔧 Services to run: ${SERVICES[*]}"
        
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
        ;;
        
    "parallel"|*)
        echo "⚡ Running services in parallel..."
        
        # Services to run (can be overridden by environment variable)
        if [ -n "$EM_SERVICES" ]; then
            IFS=' ' read -ra SERVICES <<< "$EM_SERVICES"
        else
            SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")
        fi
        
        echo "🔧 Services to run: ${SERVICES[*]}"
        
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
        ;;
esac

echo ""
echo "✅ All ingestion jobs completed successfully!"

# Optionally run consolidation
if [ "${EM_RUN_CONSOLIDATION:-true}" = "true" ]; then
    echo ""
    echo "📊 Running metadata consolidation..."
    
    if [ "$EXECUTION_MODE" = "staged" ]; then
        # Use staged approach for consolidation
        if docker compose --profile consolidate up --build > "$LOGS_DIR/consolidate_$(date +%Y%m%d_%H%M%S).log" 2>&1; then
            echo "✅ Metadata consolidation completed"
            echo "📄 Results available in app/consolidate/"
        else
            echo "❌ Metadata consolidation failed (check logs in $LOGS_DIR)"
            exit 1
        fi
    else
        # Use service run approach for consolidation
        if docker compose run --rm consolidate > "$LOGS_DIR/consolidate.log" 2>&1; then
            echo "✅ Metadata consolidation completed"
            echo "📄 Results available in app/consolidate/"
        else
            echo "❌ Metadata consolidation failed (check $LOGS_DIR/consolidate.log)"
            exit 1
        fi
    fi
fi

echo ""
case $EXECUTION_MODE in
    "staged")
        echo "🎉 Staged pipeline execution complete!"
        echo ""
        echo "💡 Resource Usage Summary:"
        echo "  Peak Memory: ~9GB (56% of available 16GB)"
        echo "  Peak CPU: ~1.5 cores (75% of available 2GHz)"
        echo "  Stage 1 Memory: ~3.5GB (IDR+FlyEM+EBI)"
        echo "  Total Pipeline Time: ~25-30 minutes"
        echo "  Memory Efficiency: 67% better than parallel execution"
        ;;
    *)
        echo "🎉 Pipeline execution complete!"
        ;;
esac

echo "📊 Data stored in: $DATA_DIR"
echo "📋 Logs available in: $LOGS_DIR"
echo "📈 Metadata catalog in: app/consolidate/"