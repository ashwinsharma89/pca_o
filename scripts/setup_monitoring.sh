#!/bin/bash

# Setup External Monitoring
# This script configures Datadog and New Relic monitoring

set -e

echo "=================================="
echo "External Monitoring Setup"
echo "=================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check for API keys
if [ -z "$DATADOG_API_KEY" ] && [ -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo -e "${RED}❌ No monitoring credentials found${NC}"
    echo "Please set at least one of:"
    echo "  - DATADOG_API_KEY"
    echo "  - NEW_RELIC_LICENSE_KEY"
    exit 1
fi

# Install dependencies
echo "Installing monitoring libraries..."
pip install ddtrace newrelic

echo -e "${GREEN}✅ Libraries installed${NC}"
echo ""

# Setup Datadog
if [ ! -z "$DATADOG_API_KEY" ]; then
    echo "Setting up Datadog..."
    
    # Create Datadog configuration
    mkdir -p datadog
    
    # Test Datadog API
    response=$(curl -s -o /dev/null -w "%{http_code}" \
        -H "DD-API-KEY: $DATADOG_API_KEY" \
        https://api.datadoghq.com/api/v1/validate)
    
    if [ "$response" == "200" ]; then
        echo -e "${GREEN}✅ Datadog API key valid${NC}"
        
        # Create dashboard
        echo "Creating Datadog dashboard..."
        python3 << 'EOF'
from src.observability.external_monitoring import external_monitoring
try:
    external_monitoring.datadog.create_dashboard()
    print("✅ Datadog dashboard created")
except Exception as e:
    print(f"⚠️  Could not create dashboard: {e}")
EOF
    else
        echo -e "${YELLOW}⚠️  Datadog API key invalid${NC}"
    fi
    
    echo ""
fi

# Setup New Relic
if [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo "Setting up New Relic..."
    
    # Create New Relic configuration
    mkdir -p newrelic
    
    # Generate configuration
    newrelic-admin generate-config $NEW_RELIC_LICENSE_KEY newrelic/newrelic.ini
    
    # Validate configuration
    if newrelic-admin validate-config newrelic/newrelic.ini > /dev/null 2>&1; then
        echo -e "${GREEN}✅ New Relic configuration valid${NC}"
    else
        echo -e "${YELLOW}⚠️  New Relic configuration issue${NC}"
    fi
    
    echo ""
fi

# Create startup scripts
echo "Creating startup scripts..."

# Datadog startup script
if [ ! -z "$DATADOG_API_KEY" ]; then
    cat > start_with_datadog.sh << 'EOF'
#!/bin/bash
# Start application with Datadog APM

export DATADOG_API_KEY=${DATADOG_API_KEY}
export DD_SERVICE="pca-agent"
export DD_ENV=${ENVIRONMENT:-production}
export DD_VERSION=${APP_VERSION:-1.0.0}

ddtrace-run python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
EOF
    chmod +x start_with_datadog.sh
    echo -e "${GREEN}✅ Created start_with_datadog.sh${NC}"
fi

# New Relic startup script
if [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    cat > start_with_newrelic.sh << 'EOF'
#!/bin/bash
# Start application with New Relic APM

export NEW_RELIC_LICENSE_KEY=${NEW_RELIC_LICENSE_KEY}
export NEW_RELIC_APP_NAME="PCA Agent"
export NEW_RELIC_ENVIRONMENT=${ENVIRONMENT:-production}
export NEW_RELIC_CONFIG_FILE=newrelic/newrelic.ini

newrelic-admin run-program python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
EOF
    chmod +x start_with_newrelic.sh
    echo -e "${GREEN}✅ Created start_with_newrelic.sh${NC}"
fi

# Both monitoring systems
if [ ! -z "$DATADOG_API_KEY" ] && [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    cat > start_with_full_monitoring.sh << 'EOF'
#!/bin/bash
# Start application with both Datadog and New Relic

export DATADOG_API_KEY=${DATADOG_API_KEY}
export DD_SERVICE="pca-agent"
export DD_ENV=${ENVIRONMENT:-production}
export DD_VERSION=${APP_VERSION:-1.0.0}

export NEW_RELIC_LICENSE_KEY=${NEW_RELIC_LICENSE_KEY}
export NEW_RELIC_APP_NAME="PCA Agent"
export NEW_RELIC_ENVIRONMENT=${ENVIRONMENT:-production}
export NEW_RELIC_CONFIG_FILE=newrelic/newrelic.ini

# Start with both APM systems
ddtrace-run newrelic-admin run-program python -m uvicorn src.api.main:app --host 0.0.0.0 --port 8000
EOF
    chmod +x start_with_full_monitoring.sh
    echo -e "${GREEN}✅ Created start_with_full_monitoring.sh${NC}"
fi

echo ""

# Display summary
echo "=================================="
echo "Setup Complete!"
echo "=================================="
echo ""

if [ ! -z "$DATADOG_API_KEY" ]; then
    echo -e "${GREEN}Datadog:${NC} Configured ✅"
    echo "  Dashboard: https://app.datadoghq.com/dashboard/"
    echo "  Start command: ./start_with_datadog.sh"
    echo ""
fi

if [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo -e "${GREEN}New Relic:${NC} Configured ✅"
    echo "  Dashboard: https://one.newrelic.com/"
    echo "  Start command: ./start_with_newrelic.sh"
    echo ""
fi

if [ ! -z "$DATADOG_API_KEY" ] && [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo -e "${GREEN}Full Monitoring:${NC} Both systems configured ✅"
    echo "  Start command: ./start_with_full_monitoring.sh"
    echo ""
fi

echo "=================================="
echo "Next Steps"
echo "=================================="
echo ""
echo "1. Start your application with monitoring:"
if [ ! -z "$DATADOG_API_KEY" ] && [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo "   ./start_with_full_monitoring.sh"
elif [ ! -z "$DATADOG_API_KEY" ]; then
    echo "   ./start_with_datadog.sh"
else
    echo "   ./start_with_newrelic.sh"
fi
echo ""
echo "2. Generate some traffic to see metrics"
echo ""
echo "3. View dashboards:"
if [ ! -z "$DATADOG_API_KEY" ]; then
    echo "   - Datadog: https://app.datadoghq.com/"
fi
if [ ! -z "$NEW_RELIC_LICENSE_KEY" ]; then
    echo "   - New Relic: https://one.newrelic.com/"
fi
echo ""

echo -e "${GREEN}✅ External monitoring setup complete!${NC}"
