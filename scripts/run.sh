#!/bin/bash
set -e

# Electron Microscopy Data Ingestion Pipeline
# ===========================================
# 
# Execution Modes (set via EM_EXECUTION_MODE environment variable):
#   background  - EPFL in background + sequential small loaders + OpenOrganelle overlap (default)
#                 Best: Optimal throughput + memory safety (12-14GB, 20-25 min) ⭐
#   staged      - Light services parallel → Heavy services sequential 
#                 Best: Constrained hardware 16GB systems (9GB peak, 25-30 min)
#   parallel    - All services simultaneously 
#                 Best: High-memory systems 32GB+ (15-20GB, 15-20 min)
#   sequential  - One service at a time 
#                 Best: Maximum memory conservation (9GB peak, 35-40 min)
#
# Examples:
#   scripts/run.sh                              # Uses background mode (default)
#   EM_EXECUTION_MODE=staged scripts/run.sh     # Use staged mode for 16GB systems
#   EM_EXECUTION_MODE=parallel scripts/run.sh   # Use parallel mode for 32GB+ systems
#   EM_EXECUTION_MODE=sequential scripts/run.sh # Use sequential mode for max conservation

# Change to project root directory (parent of scripts directory)
cd "$(dirname "$0")/.."

echo "🚀 Electron Microscopy Data Ingestion Pipeline"
echo "=============================================="

# Configuration
CONFIG_FILE="${EM_CONFIG_FILE:-./config/config.yaml}"
DATA_DIR="${EM_DATA_DIR:-./data}"
LOGS_DIR="${EM_LOGS_DIR:-./logs}"
EXECUTION_MODE="${EM_EXECUTION_MODE:-background}"    # background (default), staged, parallel, or sequential

# Create necessary directories
echo "📁 Setting up directories..."
mkdir -p "$DATA_DIR"/{ebi,epfl,flyem,idr,openorganelle}
mkdir -p "$LOGS_DIR"

# Clear existing logs for clean run
if [ -d "$LOGS_DIR" ] && [ "$(ls -A "$LOGS_DIR" 2>/dev/null)" ]; then
    echo "🧹 Clearing previous logs from $LOGS_DIR..."
    rm -f "$LOGS_DIR"/*.log
    echo "✅ Logs directory cleared"
fi

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
    "background")
        echo "🎯 Background Processing Strategy:"
        echo "  EPFL starts in background (6GB/1.0CPU)"
        echo "  Small loaders run sequentially (IDR → FlyEM → EBI, ~1-1.5GB each)"
        echo "  OpenOrganelle starts after small loaders complete (8GB/2.0CPU)"
        echo "  Total Peak Usage: ~12-14GB memory, optimized throughput"
        echo "  Optimized for: Maximum efficiency with memory safety"
        ;;
esac
echo ""

# Function to parse docker compose logs and show individual service status
show_service_status() {
    local log_file=$1
    local services_array=($2)
    
    echo ""
    echo "📊 Individual Service Results:"
    echo "────────────────────────────────"
    
    for service in "${services_array[@]}"; do
        # Check for explicit successful exit
        if grep -q "${service}-ingest.*exited with code 0" "$log_file" 2>/dev/null; then
            echo "✅ $service: Successfully completed"
        # Check for failed exit codes
        elif grep -q "${service}-ingest.*exited with code [1-9]" "$log_file" 2>/dev/null; then
            local exit_code=$(grep "${service}-ingest.*exited with code" "$log_file" | tail -1 | sed 's/.*exited with code \([0-9]*\).*/\1/')
            echo "❌ $service: Failed (exit code: $exit_code)"
        # Check for successful completion indicators (files saved, etc.)
        elif grep -q "$service-ingest.*✅.*Saved\|$service-ingest.*completed successfully\|$service-ingest.*SUCCESS" "$log_file" 2>/dev/null; then
            echo "✅ $service: Successfully completed (detected success indicators)"
        # Check for specific errors
        elif grep -q "ModuleNotFoundError.*config_manager" "$log_file" 2>/dev/null && grep -q "$service-ingest" "$log_file" 2>/dev/null; then
            echo "🔧 $service: Configuration error detected"
        elif grep -q "$service-ingest.*ERROR\|$service-ingest.*Failed\|$service-ingest.*Traceback" "$log_file" 2>/dev/null; then
            echo "❌ $service: Failed (error detected in logs)"
        # If service appears in logs but no clear status
        elif grep -q "$service-ingest" "$log_file" 2>/dev/null; then
            echo "🔄 $service: Processing (check logs for details)"
        else
            echo "⚠️  $service: Status unclear (not found in logs)"
        fi
    done
    echo ""
}

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
    echo "🚀 Starting services..."
    
    if docker compose --profile "$stage" up --build > "$stage_log" 2>&1; then
        echo "✅ $description completed successfully"
        show_service_status "$stage_log" "$services"
        echo "📋 Logs written to: $stage_log"
        return 0
    else
        echo "❌ $description failed"
        show_service_status "$stage_log" "$services"
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
            echo "🔧 EBI Entry ID: $EBI_ENTRY_ID"
            ;;
        "flyem")
            export FLYEM_INSTANCE="${FLYEM_INSTANCE:-grayscale}"
            export FLYEM_CROP_SIZE="${FLYEM_CROP_SIZE:-1000,1000,1000}"
            echo "🔧 FlyEM Instance: $FLYEM_INSTANCE, Crop Size: $FLYEM_CROP_SIZE"
            ;;
        "idr")
            export IDR_IMAGE_IDS="${IDR_IMAGE_IDS:-9846137}"
            export IDR_OUTPUT_DIR="$DATA_DIR/idr"
            echo "🔧 IDR Image IDs: $IDR_IMAGE_IDS"
            ;;
        "openorganelle")
            echo "🔧 OpenOrganelle: Processing S3 datasets"
            ;;
        "epfl")
            echo "🔧 EPFL: Processing TIFF downloads"
            ;;
    esac
    
    local service_log="$LOGS_DIR/${service}_$(date +%Y%m%d_%H%M%S).log"
    echo "📝 Logging to: $service_log"
    echo "🚀 Starting $service..."
    
    # Run the service using docker compose run --rm for accurate exit code
    if docker compose run --rm "$service" > "$service_log" 2>&1; then
        echo "✅ $service completed successfully"
        echo "📋 Logs written to: $service_log"
        return 0
    else
        echo "❌ $service failed"
        echo "📋 Check logs in: $service_log"
        
        # Show helpful error context for common issues
        if grep -q "ModuleNotFoundError.*config_manager" "$service_log" 2>/dev/null; then
            echo "🔧 Detected: Configuration module import error"
        elif grep -q "Connection refused\|timeout\|network" "$service_log" 2>/dev/null; then
            echo "🌐 Detected: Network connectivity issue"
        elif grep -q "Permission denied\|Authentication failed" "$service_log" 2>/dev/null; then
            echo "🔐 Detected: Authentication/permission issue"
        fi
        
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
            echo ""
            echo "📊 Sequential Execution Summary:"
            echo "─────────────────────────────────"
            for service in "${SERVICES[@]}"; do
                if [[ " ${failed_services[*]} " =~ " ${service} " ]]; then
                    echo "❌ $service: Failed"
                else
                    echo "✅ $service: Success"
                fi
            done
            echo ""
            echo "❌ Failed services: ${failed_services[*]}"
            exit 1
        else
            echo ""
            echo "📊 Sequential Execution Summary:"
            echo "─────────────────────────────────"
            for service in "${SERVICES[@]}"; do
                echo "✅ $service: Success"
            done
        fi
        ;;
        
    "background")
        echo "🏃 Starting background processing orchestration..."
        echo ""
        
        # Phase 1: Start EPFL in background (long-running download)
        echo "=== Phase 1: Starting EPFL in Background ==="
        echo "🔧 EPFL: Long-running download process (6GB, will run in background)"
        
        epfl_log="$LOGS_DIR/epfl_background_$(date +%Y%m%d_%H%M%S).log"
        echo "📝 EPFL logging to: $epfl_log"
        echo "🚀 Starting EPFL in background..."
        
        # Start EPFL in background and capture its PID
        docker compose run --rm epfl > "$epfl_log" 2>&1 &
        epfl_pid=$!
        echo "✅ EPFL started in background (PID: $epfl_pid)"
        echo ""
        
        # Phase 2: Run small loaders sequentially
        echo "=== Phase 2: Sequential Small Loaders ==="
        echo "🔧 Running IDR → FlyEM → EBI sequentially (low memory usage)"
        
        small_services=("idr" "flyem" "ebi")
        failed_small_services=()
        
        for service in "${small_services[@]}"; do
            echo ""
            echo "🔄 Processing $service..."
            if ! run_service "$service"; then
                failed_small_services+=("$service")
                echo "❌ $service failed, but continuing with remaining services"
            else
                echo "✅ $service completed successfully"
            fi
        done
        
        echo ""
        echo "📊 Small Loaders Phase Complete:"
        echo "─────────────────────────────────────"
        for service in "${small_services[@]}"; do
            if [[ " ${failed_small_services[*]} " =~ " ${service} " ]]; then
                echo "❌ $service: Failed"
            else
                echo "✅ $service: Success"
            fi
        done
        
        # Phase 3: Start OpenOrganelle now that small loaders are done
        echo ""
        echo "=== Phase 3: Starting OpenOrganelle ==="
        echo "🔧 OpenOrganelle: Conservative chunked processing (8GB, parallel chunks)"
        
        openorganelle_log="$LOGS_DIR/openorganelle_$(date +%Y%m%d_%H%M%S).log"
        echo "📝 OpenOrganelle logging to: $openorganelle_log"
        echo "🚀 Starting OpenOrganelle..."
        
        # Start OpenOrganelle in background too, so we can monitor both
        docker compose run --rm openorganelle > "$openorganelle_log" 2>&1 &
        openorganelle_pid=$!
        echo "✅ OpenOrganelle started (PID: $openorganelle_pid)"
        echo ""
        
        # Phase 4: Wait for both background processes
        echo "=== Phase 4: Waiting for Background Processes ==="
        echo "🔄 Monitoring EPFL (PID: $epfl_pid) and OpenOrganelle (PID: $openorganelle_pid)..."
        echo ""
        
        # Monitor processes with status updates
        epfl_finished=false
        openorganelle_finished=false
        
        while [[ "$epfl_finished" = false || "$openorganelle_finished" = false ]]; do
            # Check EPFL
            if [[ "$epfl_finished" = false ]] && ! kill -0 $epfl_pid 2>/dev/null; then
                wait $epfl_pid
                epfl_exit=$?
                if [ $epfl_exit -eq 0 ]; then
                    echo "✅ EPFL completed successfully"
                    show_service_status "$epfl_log" "epfl"
                else
                    echo "❌ EPFL failed (exit code: $epfl_exit)"
                    show_service_status "$epfl_log" "epfl"
                fi
                epfl_finished=true
            fi
            
            # Check OpenOrganelle
            if [[ "$openorganelle_finished" = false ]] && ! kill -0 $openorganelle_pid 2>/dev/null; then
                wait $openorganelle_pid
                openorganelle_exit=$?
                if [ $openorganelle_exit -eq 0 ]; then
                    echo "✅ OpenOrganelle completed successfully"
                    show_service_status "$openorganelle_log" "openorganelle"
                else
                    echo "❌ OpenOrganelle failed (exit code: $openorganelle_exit)"
                    show_service_status "$openorganelle_log" "openorganelle"
                fi
                openorganelle_finished=true
            fi
            
            # Show progress every 30 seconds if both still running
            if [[ "$epfl_finished" = false || "$openorganelle_finished" = false ]]; then
                echo "🔄 Still processing... (EPFL: $([ "$epfl_finished" = true ] && echo "✅" || echo "🔄"), OpenOrganelle: $([ "$openorganelle_finished" = true ] && echo "✅" || echo "🔄"))"
                sleep 30
            fi
        done
        
        # Final summary
        echo ""
        echo "📊 Background Processing Summary:"
        echo "─────────────────────────────────────"
        
        # Check for any failures
        background_failed=false
        if [ ${#failed_small_services[@]} -gt 0 ]; then
            echo "❌ Small services failed: ${failed_small_services[*]}"
            background_failed=true
        else
            echo "✅ Small services: All completed successfully"
        fi
        
        if [ $epfl_exit -eq 0 ]; then
            echo "✅ EPFL: Completed successfully"
        else
            echo "❌ EPFL: Failed (exit code: $epfl_exit)"
            background_failed=true
        fi
        
        if [ $openorganelle_exit -eq 0 ]; then
            echo "✅ OpenOrganelle: Completed successfully"
        else
            echo "❌ OpenOrganelle: Failed (exit code: $openorganelle_exit)"
            background_failed=true
        fi
        
        if [ "$background_failed" = true ]; then
            echo ""
            echo "❌ Some services failed. Check logs in $LOGS_DIR"
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
        failed_services=()
        successful_services=()
        
        for i in "${!pids[@]}"; do
            if wait "${pids[$i]}"; then
                successful_services+=("${SERVICES[$i]}")
            else
                failed_services+=("${SERVICES[$i]}")
            fi
        done
        
        echo ""
        echo "📊 Parallel Execution Summary:"
        echo "──────────────────────────────"
        for service in "${SERVICES[@]}"; do
            if [[ " ${failed_services[*]} " =~ " ${service} " ]]; then
                echo "❌ $service: Failed"
            else
                echo "✅ $service: Success"
            fi
        done
        
        if [ ${#failed_services[@]} -gt 0 ]; then
            echo ""
            echo "❌ Failed services: ${failed_services[*]}"
            echo "✅ Successful services: ${successful_services[*]}"
            echo "📋 Check logs in $LOGS_DIR"
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
            echo "📄 Results available in metadata/"
        else
            echo "❌ Metadata consolidation failed (check logs in $LOGS_DIR)"
            exit 1
        fi
    else
        # Use service run approach for consolidation
        if docker compose run --rm consolidate > "$LOGS_DIR/consolidate.log" 2>&1; then
            echo "✅ Metadata consolidation completed"
            echo "📄 Results available in metadata/"
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
    "background")
        echo "🎉 Background orchestrated pipeline execution complete!"
        echo ""
        echo "💡 Orchestration Summary:"
        echo "  Strategy: EPFL in background + Sequential small loaders + OpenOrganelle overlap"
        echo "  Peak Memory: ~12-14GB (87% of available 16GB)"
        echo "  Efficiency: Maximized throughput with optimal memory usage"
        echo "  Small Loaders: IDR(1GB) → FlyEM(1.5GB) → EBI(1GB) - Sequential"
        echo "  Heavy Loaders: EPFL(6GB, background) + OpenOrganelle(8GB, chunked)"
        echo "  Total Pipeline Time: ~20-25 minutes (optimized overlap)"
        ;;
    *)
        echo "🎉 Pipeline execution complete!"
        ;;
esac

echo "📊 Data stored in: $DATA_DIR"
echo "📋 Logs available in: $LOGS_DIR"
echo "📈 Metadata catalog in: metadata/"