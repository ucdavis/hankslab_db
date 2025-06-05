#!/bin/bash

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Adjust this path to your Miniconda or Anaconda install
CONDA_BASE="$HOME/miniconda3"

# Initialize conda for bash (modify if you use zsh or other shell)
# This enables 'conda' command inside the script
source "$CONDA_BASE/etc/profile.d/conda.sh"

# Activate the environment
conda activate neuropy

# Run the python script
python "$SCRIPT_DIR/behavior_tracker.py"