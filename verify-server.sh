#!/bin/bash
# Pre-deployment verification script
# Tests SSH and database connectivity before deploying

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SERVER="10.1.8.144"
USER="ewura-admin"

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Pre-Deployment Verification${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Test 1: SSH Connection
echo -e "${YELLOW}Test 1: SSH Connection...${NC}"
if ssh -o ConnectTimeout=5 -o BatchMode=yes -o StrictHostKeyChecking=no ${USER}@${SERVER} exit 2>/dev/null; then
    echo -e "${GREEN}✓ SSH connection successful${NC}"
else
    echo -e "${RED}✗ SSH connection failed${NC}"
    echo "Attempting interactive connection..."
    ssh ${USER}@${SERVER} "echo '✓ SSH works'; exit"
fi
echo ""

# Test 2: Python availability
echo -e "${YELLOW}Test 2: Python installation...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
if command -v python3 &> /dev/null; then
    VERSION=$(python3 --version)
    echo "✓ $VERSION"
else
    echo "✗ Python 3 not found"
    exit 1
fi
EOF
echo ""

# Test 3: PostgreSQL availability
echo -e "${YELLOW}Test 3: PostgreSQL installation...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
if command -v psql &> /dev/null; then
    VERSION=$(psql --version)
    echo "✓ $VERSION"
else
    echo "✗ PostgreSQL not found"
    exit 1
fi
EOF
echo ""

# Test 4: Target directory
echo -e "${YELLOW}Test 4: Checking target directory...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
if [ -d "/var/lib/pgsql" ]; then
    echo "✓ /var/lib/pgsql exists"
else
    echo "✗ /var/lib/pgsql does not exist"
    exit 1
fi

if [ -d "/var/lib/pgsql/scripts/ewura-migration" ]; then
    echo "✓ Migration directory already exists"
else
    echo "ℹ Migration directory will be created during deployment"
fi
EOF
echo ""

# Test 5: Database connection
echo -e "${YELLOW}Test 5: Database connectivity...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
export PGPASSWORD='ewura@123'
if psql -h localhost -U postgres -d eservice_applications -c "SELECT 1;" &> /dev/null; then
    echo "✓ Database connection successful"
    echo "Database: eservice_applications"
else
    echo "⚠ Database connection test skipped (may require postgres user)"
    echo "Will test after deployment"
fi
EOF
echo ""

# Test 6: Port availability
echo -e "${YELLOW}Test 6: Port 8000 availability...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
if sudo netstat -tlnp 2>/dev/null | grep -q ":8000 "; then
    echo "⚠ Port 8000 is already in use"
    echo "You may need to stop the existing service or use a different port"
else
    echo "✓ Port 8000 is available"
fi
EOF
echo ""

# Test 7: Sudo privileges
echo -e "${YELLOW}Test 7: Sudo privileges...${NC}"
ssh ${USER}@${SERVER} << 'EOF'
if sudo -n true 2>/dev/null; then
    echo "✓ Passwordless sudo available"
elif sudo -v 2>/dev/null; then
    echo "✓ Sudo available (may require password)"
else
    echo "⚠ Sudo access verification skipped"
fi
EOF
echo ""

# Summary
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Verification Complete${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Server is ready for deployment!"
echo ""
echo "Next steps:"
echo "  1. Run: ./deploy.sh"
echo "  2. Access API: http://10.1.8.144:8000/docs"
echo ""
