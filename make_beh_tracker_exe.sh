#!/bin/bash

# Exit if any command fails
set -e

# Get the directory this script is in
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# File to make executable
TARGET_SCRIPT="$SCRIPT_DIR/launch_beh_tracker.command"

# Set the executable bit
chmod +x "$TARGET_SCRIPT"