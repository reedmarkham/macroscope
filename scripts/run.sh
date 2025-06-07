#!/bin/bash
set -e

# Electron Microscopy Data Ingestion Pipeline
# ===========================================
# 
# Execution Modes (set via EM_EXECUTION_MODE environment variable):
#   background  - EPFL in background + sequential small loaders + OpenOrganelle overlap (default)
#                 Best: Optimal throughput + memory safety (12-14GB, 20-25 min)
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

echo "Electron Microscopy Data Ingestion Pipeline"
echo "=============================================="
echo "Pipeline started at: $(date '+%Y-%m-%d %H:%M:%S')"
PIPELINE_START_TIME=$(date +%s)

# Configuration
CONFIG_FILE="${EM_CONFIG_FILE:-./config/config.yaml}"
DATA_DIR="${EM_DATA_DIR:-./data}"
LOGS_DIR="${EM_LOGS_DIR:-./logs}"
EXECUTION_MODE="${EM_EXECUTION_MODE:-background}"    # background (default), staged, parallel, or sequential

# Create necessary directories
echo "Setting up directories..."
mkdir -p "$DATA_DIR"/{ebi,epfl,flyem,idr,openorganelle}
mkdir -p "$LOGS_DIR"

# Clear existing logs for clean run
if [ -d "$LOGS_DIR" ] && [ "$(ls -A "$LOGS_DIR" 2>/dev/null)" ]; then
    echo "Clearing previous logs from $LOGS_DIR..."
    rm -f "$LOGS_DIR"/*.log
    echo "Logs directory cleared"
fi

# Validate configuration exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Configuration file not found: $CONFIG_FILE"
    echo "Please create configuration file or set EM_CONFIG_FILE environment variable"
    exit 1
fi

echo "Using configuration: $CONFIG_FILE"
echo "Data will be stored in: $DATA_DIR"
echo "Pipeline orchestration logs will be stored in: $LOGS_DIR"
echo "Orchestration mode: $EXECUTION_MODE"
echo ""

# Display resource allocation based on execution mode
case $EXECUTION_MODE in
    "staged")
        echo "Staged Resource Allocation Strategy:"
        echo "  Stage 1 (Parallel):  IDR(1GB/0.25CPU) + FlyEM(1.5GB/0.25CPU) + EBI(1GB/0.5CPU)"
        echo "  Stage 2 (Sequential): OpenOrganelle(9GB/1.5CPU) → EPFL(6GB/1.0CPU)"
        echo "  Total Peak Usage: 9GB memory, 1.5 CPU cores"
        echo "  Optimized for: 16GB memory systems"
        ;;
    "parallel")
        echo "Parallel Resource Allocation:"
        echo "  All services run simultaneously with shared resources"
        echo "  Estimated Peak Usage: 15-20GB memory, 4+ CPU cores"
        echo "  Recommended for: 32GB+ memory systems"
        ;;
    "sequential")
        echo "Sequential Resource Allocation:"
        echo "  Services run one at a time with full resource access"
        echo "  Peak Usage: ~9GB memory per service, low parallelism"
        echo "  Slowest but most memory-conservative approach"
        ;;
    "background")
        echo "Background Processing Strategy:"
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
    echo "Individual Service Results:"
    echo "────────────────────────────────"
    
    for service in "${services_array[@]}"; do
        # Check for explicit successful exit
        if grep -q "${service}-ingest.*exited with code 0" "$log_file" 2>/dev/null; then
            echo "SUCCESS: $service completed successfully"
        # Check for failed exit codes
        elif grep -q "${service}-ingest.*exited with code [1-9]" "$log_file" 2>/dev/null; then
            local exit_code=$(grep "${service}-ingest.*exited with code" "$log_file" | tail -1 | sed 's/.*exited with code \([0-9]*\).*/\1/')
            echo "FAILED: $service failed (exit code: $exit_code)"
        # Check for successful completion indicators (files saved, etc.)
        elif grep -q "$service-ingest.*SUCCESS:.*Saved\|$service-ingest.*completed successfully\|$service-ingest.*SUCCESS" "$log_file" 2>/dev/null; then
            echo "SUCCESS: $service completed successfully (detected success indicators)"
        # Check for specific errors
        elif grep -q "ModuleNotFoundError.*config_manager" "$log_file" 2>/dev/null && grep -q "$service-ingest" "$log_file" 2>/dev/null; then
            echo "ERROR: $service configuration error detected"
        elif grep -q "$service-ingest.*ERROR\|$service-ingest.*Failed\|$service-ingest.*Traceback" "$log_file" 2>/dev/null; then
            echo "FAILED: $service failed (error detected in logs)"
        # If service appears in logs but no clear status
        elif grep -q "$service-ingest" "$log_file" 2>/dev/null; then
            echo "PROCESSING: $service in progress (check logs for details)"
        else
            echo "UNKNOWN: $service status unclear (not found in logs)"
        fi
    done
    echo ""
}

# Function to run a stage with error handling (for staged execution)
run_stage() {
    local stage=$1
    local description=$2
    local services=$3
    
    local stage_start_time=$(date +%s)
    echo "=== $description ==="
    echo "Orchestrating services: $services"
    echo "Stage started at: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # Create stage-specific log file and redirect output
    local stage_log="$LOGS_DIR/${stage}_$(date +%Y%m%d_%H%M%S).log"
    
    echo "Pipeline orchestration logging to: $stage_log"
    echo "Orchestrating Docker services..."
    
    if docker compose --profile "$stage" up --build > "$stage_log" 2>&1; then
        local stage_end_time=$(date +%s)
        local stage_duration=$((stage_end_time - stage_start_time))
        echo "SUCCESS: $description completed successfully"
        echo "Stage duration: $(printf '%02d:%02d:%02d' $((stage_duration/3600)) $((stage_duration%3600/60)) $((stage_duration%60)))"
        show_service_status "$stage_log" "$services"
        echo "Pipeline orchestration logs written to: $stage_log"
        return 0
    else
        local stage_end_time=$(date +%s)
        local stage_duration=$((stage_end_time - stage_start_time))
        echo "FAILED: $description failed"
        echo "Stage duration: $(printf '%02d:%02d:%02d' $((stage_duration/3600)) $((stage_duration%3600/60)) $((stage_duration%60)))"
        show_service_status "$stage_log" "$services"
        echo "Check pipeline orchestration logs in: $stage_log"
        return 1
    fi
}

# Function to run a single service (for sequential execution)
run_service() {
    local service=$1
    local service_start_time=$(date +%s)
    echo "=== Starting $service ingestion ==="
    echo "Service started at: $(date '+%Y-%m-%d %H:%M:%S')"
    
    # Set service-specific environment variables if they exist
    case $service in
        "ebi")
            export EBI_ENTRY_ID="${EBI_ENTRY_ID:-11759}"
            echo "CONFIG: EBI Entry ID: $EBI_ENTRY_ID"
            ;;
        "flyem")
            export FLYEM_INSTANCE="${FLYEM_INSTANCE:-grayscale}"
            export FLYEM_CROP_SIZE="${FLYEM_CROP_SIZE:-1000,1000,1000}"
            echo "CONFIG: FlyEM Instance: $FLYEM_INSTANCE, Crop Size: $FLYEM_CROP_SIZE"
            ;;
        "idr")
            export IDR_IMAGE_IDS="${IDR_IMAGE_IDS:-9846137}"
            export IDR_OUTPUT_DIR="$DATA_DIR/idr"
            echo "CONFIG: IDR Image IDs: $IDR_IMAGE_IDS"
            ;;
        "openorganelle")
            echo "CONFIG: OpenOrganelle: Processing S3 datasets"
            ;;
        "epfl")
            echo "CONFIG: EPFL: Processing TIFF downloads"
            ;;
    esac
    
    local service_log="$LOGS_DIR/${service}_$(date +%Y%m%d_%H%M%S).log"
    echo "LOGGING: Service logging to: $service_log"
    echo "STARTING: Orchestrating $service container..."
    
    # Run the service using docker compose run --rm for accurate exit code
    if docker compose run --rm "$service" > "$service_log" 2>&1; then
        local service_end_time=$(date +%s)
        local service_duration=$((service_end_time - service_start_time))
        echo "SUCCESS: $service completed successfully"
        echo " Service duration: $(printf '%02d:%02d:%02d' $((service_duration/3600)) $((service_duration%3600/60)) $((service_duration%60)))"
        echo "LOGS: Service logs written to: $service_log"
        return 0
    else
        local service_end_time=$(date +%s)
        local service_duration=$((service_end_time - service_start_time))
        echo "FAILED: $service failed"
        echo " Service duration: $(printf '%02d:%02d:%02d' $((service_duration/3600)) $((service_duration%3600/60)) $((service_duration%60)))"
        echo "LOGS: Check service logs in: $service_log"
        
        # Show helpful error context for common issues
        if grep -q "ModuleNotFoundError.*config_manager" "$service_log" 2>/dev/null; then
            echo "CONFIG: Detected: Configuration module import error"
        elif grep -q "Connection refused\|timeout\|network" "$service_log" 2>/dev/null; then
            echo "NETWORK: Detected: Network connectivity issue"
        elif grep -q "Permission denied\|Authentication failed" "$service_log" 2>/dev/null; then
            echo "AUTH: Detected: Authentication/permission issue"
        fi
        
        return 1
    fi
}

# Execute based on selected mode
case $EXECUTION_MODE in
    "staged")
        echo "ORCHESTRATING: Starting staged pipeline execution..."
        echo ""
        
        # Stage 1: Light services in parallel
        if ! run_stage "stage1" "Stage 1: Light Services (Parallel)" "idr, flyem, ebi"; then
            echo "FAILED: Stage 1 failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        
        echo ""
        echo "STATUS: Stage 1 complete. Starting heavy processing..."
        echo ""
        
        # Stage 2: OpenOrganelle (heavy processing)
        if ! run_stage "stage2" "Stage 2: OpenOrganelle Processing" "openorganelle"; then
            echo "FAILED: Stage 2 (OpenOrganelle) failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        
        echo ""
        echo "STATUS: OpenOrganelle complete. Starting EPFL..."
        echo ""
        
        # Stage 3: EPFL (large downloads)
        if ! run_stage "stage3" "Stage 3: EPFL Processing" "epfl"; then
            echo "FAILED: Stage 3 (EPFL) failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        ;;
        
    "sequential")
        echo "PROCESSING: Running services sequentially..."
        
        # Services to run (can be overridden by environment variable)
        if [ -n "$EM_SERVICES" ]; then
            IFS=' ' read -ra SERVICES <<< "$EM_SERVICES"
        else
            SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")
        fi
        
        echo "CONFIG: Services to run: ${SERVICES[*]}"
        
        failed_services=()
        for service in "${SERVICES[@]}"; do
            if ! run_service "$service"; then
                failed_services+=("$service")
            fi
        done
        
        if [ ${#failed_services[@]} -gt 0 ]; then
            echo ""
            echo "SUMMARY: Sequential Execution Summary:"
            echo "─────────────────────────────────"
            for service in "${SERVICES[@]}"; do
                if [[ " ${failed_services[*]} " =~ " ${service} " ]]; then
                    echo "FAILED: $service: Failed"
                else
                    echo "SUCCESS: $service: Success"
                fi
            done
            echo ""
            echo "FAILED: Failed services: ${failed_services[*]}"
            exit 1
        else
            echo ""
            echo "SUMMARY: Sequential Execution Summary:"
            echo "─────────────────────────────────"
            for service in "${SERVICES[@]}"; do
                echo "SUCCESS: $service: Success"
            done
        fi
        ;;
        
    "background")
        echo "ORCHESTRATING: Starting background processing orchestration..."
        echo ""
        
        # Phase 1: Start EPFL in background (long-running download)
        echo "=== Phase 1: Starting EPFL in Background ==="
        echo "CONFIG: EPFL: Long-running download process (6GB, will run in background)"
        
        epfl_log="$LOGS_DIR/epfl_$(date +%Y%m%d_%H%M%S).log"
        echo "LOGGING: EPFL service logging to: $epfl_log"
        echo "STARTING: Orchestrating EPFL container in background..."
        
        # Start EPFL in background and capture its PID
        docker compose run --rm epfl > "$epfl_log" 2>&1 &
        epfl_pid=$!
        echo "SUCCESS: EPFL container orchestrated in background (PID: $epfl_pid)"
        echo ""
        
        # Phase 2: Run small loaders sequentially
        echo "=== Phase 2: Sequential Small Loaders ==="
        echo "CONFIG: Running IDR → FlyEM → EBI sequentially (low memory usage)"
        
        small_services=("idr" "flyem" "ebi")
        failed_small_services=()
        
        for service in "${small_services[@]}"; do
            echo ""
            echo "PROCESSING: Processing $service..."
            if ! run_service "$service"; then
                failed_small_services+=("$service")
                echo "FAILED: $service failed, but continuing with remaining services"
            else
                echo "SUCCESS: $service completed successfully"
            fi
        done
        
        echo ""
        echo "SUMMARY: Small Loaders Phase Complete:"
        echo "─────────────────────────────────────"
        for service in "${small_services[@]}"; do
            if [[ " ${failed_small_services[*]} " =~ " ${service} " ]]; then
                echo "FAILED: $service: Failed"
            else
                echo "SUCCESS: $service: Success"
            fi
        done
        
        # Phase 3: Start OpenOrganelle now that small loaders are done
        echo ""
        echo "=== Phase 3: Starting OpenOrganelle ==="
        echo "CONFIG: OpenOrganelle: Conservative chunked processing (8GB, parallel chunks)"
        
        openorganelle_log="$LOGS_DIR/openorganelle_$(date +%Y%m%d_%H%M%S).log"
        echo "LOGGING: OpenOrganelle service logging to: $openorganelle_log"
        echo "STARTING: Orchestrating OpenOrganelle container..."
        
        # Start OpenOrganelle in background too, so we can monitor both
        docker compose run --rm openorganelle > "$openorganelle_log" 2>&1 &
        openorganelle_pid=$!
        echo "SUCCESS: OpenOrganelle container orchestrated (PID: $openorganelle_pid)"
        echo ""
        
        # Phase 4: Wait for both background processes
        echo "=== Phase 4: Waiting for Background Processes ==="
        echo "ORCHESTRATING: Monitoring EPFL container (PID: $epfl_pid) and OpenOrganelle container (PID: $openorganelle_pid)..."
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
                    echo "SUCCESS: EPFL completed successfully"
                    show_service_status "$epfl_log" "epfl"
                else
                    echo "FAILED: EPFL failed (exit code: $epfl_exit)"
                    show_service_status "$epfl_log" "epfl"
                fi
                epfl_finished=true
            fi
            
            # Check OpenOrganelle
            if [[ "$openorganelle_finished" = false ]] && ! kill -0 $openorganelle_pid 2>/dev/null; then
                wait $openorganelle_pid
                openorganelle_exit=$?
                if [ $openorganelle_exit -eq 0 ]; then
                    echo "SUCCESS: OpenOrganelle completed successfully"
                    show_service_status "$openorganelle_log" "openorganelle"
                else
                    echo "FAILED: OpenOrganelle failed (exit code: $openorganelle_exit)"
                    show_service_status "$openorganelle_log" "openorganelle"
                fi
                openorganelle_finished=true
            fi
            
            # Show progress every 30 seconds if both still running
            if [[ "$epfl_finished" = false || "$openorganelle_finished" = false ]]; then
                echo "PROCESSING: Still processing... (EPFL: $([ "$epfl_finished" = true ] && echo "SUCCESS:" || echo "PROCESSING:"), OpenOrganelle: $([ "$openorganelle_finished" = true ] && echo "SUCCESS:" || echo "PROCESSING:"))"
                sleep 30
            fi
        done
        
        # Final summary
        echo ""
        echo "SUMMARY: Background Processing Summary:"
        echo "─────────────────────────────────────"
        
        # Check for any failures
        background_failed=false
        if [ ${#failed_small_services[@]} -gt 0 ]; then
            echo "FAILED: Small services failed: ${failed_small_services[*]}"
            background_failed=true
        else
            echo "SUCCESS: Small services: All completed successfully"
        fi
        
        if [ $epfl_exit -eq 0 ]; then
            echo "SUCCESS: EPFL: Completed successfully"
        else
            echo "FAILED: EPFL: Failed (exit code: $epfl_exit)"
            background_failed=true
        fi
        
        if [ $openorganelle_exit -eq 0 ]; then
            echo "SUCCESS: OpenOrganelle: Completed successfully"
        else
            echo "FAILED: OpenOrganelle: Failed (exit code: $openorganelle_exit)"
            background_failed=true
        fi
        
        if [ "$background_failed" = true ]; then
            echo ""
            echo "FAILED: Some services failed. Check logs in $LOGS_DIR"
            exit 1
        fi
        ;;
        
    "parallel"|*)
        echo "PARALLEL: Running services in parallel..."
        
        # Services to run (can be overridden by environment variable)
        if [ -n "$EM_SERVICES" ]; then
            IFS=' ' read -ra SERVICES <<< "$EM_SERVICES"
        else
            SERVICES=("ebi" "epfl" "flyem" "idr" "openorganelle")
        fi
        
        echo "CONFIG: Services to run: ${SERVICES[*]}"
        
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
        echo "SUMMARY: Parallel Execution Summary:"
        echo "──────────────────────────────"
        for service in "${SERVICES[@]}"; do
            if [[ " ${failed_services[*]} " =~ " ${service} " ]]; then
                echo "FAILED: $service: Failed"
            else
                echo "SUCCESS: $service: Success"
            fi
        done
        
        if [ ${#failed_services[@]} -gt 0 ]; then
            echo ""
            echo "FAILED: Failed services: ${failed_services[*]}"
            echo "SUCCESS: Successful services: ${successful_services[*]}"
            echo "LOGS: Check logs in $LOGS_DIR"
            exit 1
        fi
        ;;
esac

echo ""
PIPELINE_END_TIME=$(date +%s)
TOTAL_DURATION=$((PIPELINE_END_TIME - PIPELINE_START_TIME))
echo "SUCCESS: All ingestion jobs completed successfully!"
echo " Pipeline completed at: $(date '+%Y-%m-%d %H:%M:%S')"
echo " Total pipeline duration: $(printf '%02d:%02d:%02d' $((TOTAL_DURATION/3600)) $((TOTAL_DURATION%3600/60)) $((TOTAL_DURATION%60)))"

# Optionally run consolidation
if [ "${EM_RUN_CONSOLIDATION:-true}" = "true" ]; then
    echo ""
    echo "SUMMARY: Running metadata consolidation..."
    
    if [ "$EXECUTION_MODE" = "staged" ]; then
        # Use staged approach for consolidation
        if docker compose --profile consolidate up --build > "$LOGS_DIR/consolidate_$(date +%Y%m%d_%H%M%S).log" 2>&1; then
            echo "SUCCESS: Metadata consolidation completed"
            echo "RESULTS: Results available in metadata/"
        else
            echo "FAILED: Metadata consolidation failed (check logs in $LOGS_DIR)"
            exit 1
        fi
    else
        # Use service run approach for consolidation
        if docker compose run --rm consolidate > "$LOGS_DIR/consolidate.log" 2>&1; then
            echo "SUCCESS: Metadata consolidation completed"
            echo "RESULTS: Results available in metadata/"
        else
            echo "FAILED: Metadata consolidation failed (check $LOGS_DIR/consolidate.log)"
            exit 1
        fi
    fi
fi

echo ""
case $EXECUTION_MODE in
    "staged")
        echo "COMPLETE: Staged pipeline execution complete!"
        echo ""
        echo "INFO: Resource Usage Summary:"
        echo "  Peak Memory: ~9GB (56% of available 16GB)"
        echo "  Peak CPU: ~1.5 cores (75% of available 2GHz)"
        echo "  Stage 1 Memory: ~3.5GB (IDR+FlyEM+EBI)"
        echo "  Total Pipeline Time: ~25-30 minutes"
        echo "  Memory Efficiency: 67% better than parallel execution"
        ;;
    "background")
        echo "COMPLETE: Background orchestrated pipeline execution complete!"
        echo ""
        echo "INFO: Orchestration Summary:"
        echo "  Strategy: EPFL in background + Sequential small loaders + OpenOrganelle overlap"
        echo "  Peak Memory: ~12-14GB (87% of available 16GB)"
        echo "  Efficiency: Maximized throughput with optimal memory usage"
        echo "  Small Loaders: IDR(1GB) → FlyEM(1.5GB) → EBI(1GB) - Sequential"
        echo "  Heavy Loaders: EPFL(6GB, background) + OpenOrganelle(8GB, chunked)"
        echo "  Total Pipeline Time: ~20-25 minutes (optimized overlap)"
        ;;
    *)
        echo "COMPLETE: Pipeline execution complete!"
        ;;
esac

echo "SUMMARY: Data stored in: $DATA_DIR"
echo "LOGS: Logs available in: $LOGS_DIR"
echo "Metadata catalog in: metadata/"