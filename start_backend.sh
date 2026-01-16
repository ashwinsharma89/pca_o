#!/bin/bash
echo "Starting PCA Agent API..."
source .env
python3 -m src.api.main
