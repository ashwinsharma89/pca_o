#!/bin/bash
# Script to fix requirements.txt - add missing dependencies and remove duplicates

BACKEND_DIR="$HOME/Desktop/pca_agent_copy"
REQ_FILE="$BACKEND_DIR/requirements.txt"

echo "🔧 Fixing requirements.txt..."

if [ ! -f "$REQ_FILE" ]; then
    echo "❌ Error: requirements.txt not found at $REQ_FILE"
    exit 1
fi

# Backup original
cp "$REQ_FILE" "$REQ_FILE.backup"

# Add loguru if missing
if ! grep -q "^loguru" "$REQ_FILE"; then
    echo "loguru>=0.7.0" >> "$REQ_FILE"
    echo "✅ Added loguru"
fi

# Remove duplicate gunicorn entries and keep only one
grep -v "^gunicorn" "$REQ_FILE" > "$REQ_FILE.tmp"
echo "gunicorn>=21.2.0" >> "$REQ_FILE.tmp"
mv "$REQ_FILE.tmp" "$REQ_FILE"
echo "✅ Fixed gunicorn duplicates"

# Sort and remove empty lines
grep -v "^$" "$REQ_FILE" | sort -u > "$REQ_FILE.tmp"
mv "$REQ_FILE.tmp" "$REQ_FILE"

echo ""
echo "✅ Requirements fixed!"
echo "📋 Missing dependencies added:"
echo "   - loguru>=0.7.0"
echo ""
echo "📄 Backup saved to: $REQ_FILE.backup"
