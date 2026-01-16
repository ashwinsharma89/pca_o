#!/bin/bash
PROJECT_DIR="/Users/ashwin/Desktop/pca_agent copy"
cd "$PROJECT_DIR"
./start_all.sh
# Keep terminal open if there's an error, otherwise it can close
echo "Press any key to close this terminal window..."
read -n 1
