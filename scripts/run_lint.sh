#!/bin/bash

# run_lint.sh - Local code quality checker
# Runs all code quality tools used in CI/CD pipeline

set -e

# Change to project root directory (parent of scripts directory)
cd "$(dirname "$0")/.."

echo "========================================"
echo "Running Code Quality Checks"
echo "========================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored status
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✓ $2 passed${NC}"
        return 0
    else
        echo -e "${RED}✗ $2 failed${NC}"
        return 1
    fi
}

# Check if required tools are installed
echo -e "${BLUE}Checking required tools...${NC}"
MISSING_TOOLS=()

for tool in pylint black isort flake8; do
    if ! command -v $tool &> /dev/null; then
        MISSING_TOOLS+=($tool)
    fi
done

if [ ${#MISSING_TOOLS[@]} -ne 0 ]; then
    echo -e "${RED}Missing tools: ${MISSING_TOOLS[*]}${NC}"
    echo "Install with: pip install pylint black isort flake8"
    exit 1
fi

echo -e "${GREEN}All tools found${NC}"
echo ""

# Track overall status
OVERALL_STATUS=0

# 1. Black format check
echo -e "${BLUE}1. Running Black format check...${NC}"
if black --check --diff . &> black.log; then
    print_status 0 "Black formatting"
else
    print_status 1 "Black formatting"
    echo -e "${YELLOW}Run 'black .' to fix formatting issues${NC}"
    OVERALL_STATUS=1
fi

# 2. isort import check
echo -e "${BLUE}2. Running isort import check...${NC}"
if isort --check-only --diff . &> isort.log; then
    print_status 0 "isort imports"
else
    print_status 1 "isort imports"
    echo -e "${YELLOW}Run 'isort .' to fix import issues${NC}"
    OVERALL_STATUS=1
fi

# 3. Flake8 style check
echo -e "${BLUE}3. Running Flake8 style check...${NC}"
if flake8 . --count --statistics &> flake8.log; then
    print_status 0 "Flake8 style"
else
    print_status 1 "Flake8 style"
    echo -e "${YELLOW}Check flake8.log for details${NC}"
    OVERALL_STATUS=1
fi

# 4. Pylint analysis
echo -e "${BLUE}4. Running Pylint analysis...${NC}"
mkdir -p pylint-output

# Run Pylint on lib/
echo "  Analyzing lib/ directory..."
if pylint lib/ --output-format=text --reports=yes --score=yes > pylint-output/lib-report.txt 2>&1; then
    LIB_STATUS=0
else
    LIB_STATUS=1
fi

# Run Pylint on app/
echo "  Analyzing app/ directory..."
if pylint app/ --output-format=text --reports=yes --score=yes > pylint-output/app-report.txt 2>&1; then
    APP_STATUS=0
else
    APP_STATUS=1
fi

# Extract scores
LIB_SCORE=$(grep "Your code has been rated at" pylint-output/lib-report.txt | grep -o '[0-9]\+\.[0-9]\+' | head -1 || echo "N/A")
APP_SCORE=$(grep "Your code has been rated at" pylint-output/app-report.txt | grep -o '[0-9]\+\.[0-9]\+' | head -1 || echo "N/A")

echo "  Lib Score: $LIB_SCORE/10.0"
echo "  App Score: $APP_SCORE/10.0"

if [ "$LIB_SCORE" != "N/A" ] && [ "$APP_SCORE" != "N/A" ]; then
    # Consider anything above 7.0 as passing
    if (( $(echo "$LIB_SCORE >= 7.0" | bc -l) )) && (( $(echo "$APP_SCORE >= 7.0" | bc -l) )); then
        print_status 0 "Pylint analysis"
    else
        print_status 1 "Pylint analysis (scores below 7.0)"
        OVERALL_STATUS=1
    fi
else
    print_status 1 "Pylint analysis (failed to get scores)"
    OVERALL_STATUS=1
fi

# Generate summary report
echo -e "${BLUE}5. Generating summary report...${NC}"
cat > pylint-output/local-summary.txt << EOF
=== Local Code Quality Report ===
Generated on: $(date)
Directory: $(pwd)

=== SCORES ===
Lib Score: $LIB_SCORE/10.0
App Score: $APP_SCORE/10.0

=== LOG FILES ===
- Black check: black.log
- isort check: isort.log  
- Flake8 check: flake8.log
- Pylint lib: pylint-output/lib-report.txt
- Pylint app: pylint-output/app-report.txt

=== QUICK FIXES ===
Format code: black .
Sort imports: isort .
View detailed reports: cat pylint-output/*.txt
EOF

echo "Summary report saved to: pylint-output/local-summary.txt"

# Final status
echo ""
echo "========================================"
if [ $OVERALL_STATUS -eq 0 ]; then
    echo -e "${GREEN}✓ All code quality checks passed!${NC}"
else
    echo -e "${RED}✗ Some code quality checks failed${NC}"
    echo -e "${YELLOW}Check the log files for details${NC}"
fi
echo "========================================"

exit $OVERALL_STATUS