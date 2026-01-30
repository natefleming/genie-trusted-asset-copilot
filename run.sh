#!/usr/bin/env bash
#
# Shell script wrapper for genie-trusted-asset-copilot CLI tool
# This script activates the uv-managed virtual environment and runs the tool
#
# Usage:
#   ./run.sh --space-id abc123 --catalog main --schema genie_functions --dry-run
#   ./run.sh --space-id abc123 --catalog main --schema genie_functions --warehouse-id xyz789
#

set -e  # Exit on error

# Color output for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check if virtual environment exists
if [ ! -d "$SCRIPT_DIR/.venv" ]; then
    echo -e "${RED}Error: Virtual environment not found at $SCRIPT_DIR/.venv${NC}"
    echo ""
    echo "Please run the following command first to install dependencies:"
    echo -e "${GREEN}  cd $SCRIPT_DIR && uv sync${NC}"
    echo ""
    exit 1
fi

# Check if Python executable exists in venv
if [ ! -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    echo -e "${RED}Error: Python executable not found in virtual environment${NC}"
    echo ""
    echo "The virtual environment appears to be corrupted. Please reinstall:"
    echo -e "${GREEN}  cd $SCRIPT_DIR && rm -rf .venv && uv sync${NC}"
    echo ""
    exit 1
fi

# Check if genie-trusted-asset-copilot is installed
if [ ! -f "$SCRIPT_DIR/.venv/bin/genie-trusted-asset-copilot" ]; then
    echo -e "${RED}Error: genie-trusted-asset-copilot command not found in virtual environment${NC}"
    echo ""
    echo "Please reinstall dependencies:"
    echo -e "${GREEN}  cd $SCRIPT_DIR && uv sync${NC}"
    echo ""
    exit 1
fi

# Activate virtual environment and run the CLI tool
source "$SCRIPT_DIR/.venv/bin/activate"

# Run the CLI tool with all arguments passed through
exec genie-trusted-asset-copilot "$@"
