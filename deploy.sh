#!/bin/bash
# EWURA Migration API Deployment Script
# Deploys to server 10.1.8.144:/var/lib/pgsql/scripts/ewura-migration

set -e  # Exit on any error

# Configuration
SERVER="10.1.8.144"
SERVER_USER="ewura-admin"  # Administrator user
SERVER_PASS="secure@123"     # Server password (for reference)
DEPLOY_PATH="/var/lib/pgsql/scripts/ewura-migration"
LOCAL_PROJECT_PATH="/Users/lambert/Desktop/fast-api/ewura-migration"
POSTGRES_PASSWORD="ewura@123"  # Postgres user password

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}EWURA Migration API Deployment${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Check if we're in the right directory
if [ ! -f "app/main.py" ]; then
    echo -e "${RED}Error: Must run from project root (/Users/lambert/Desktop/fast-api/ewura-migration)${NC}"
    exit 1
fi

echo -e "${YELLOW}Step 1: Creating deployment package...${NC}"
tar --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    --exclude='.env' \
    --exclude='*.tar.gz' \
    -czf ewura-migration.tar.gz .
echo -e "${GREEN}✓ Package created${NC}"
echo ""

echo -e "${YELLOW}Step 2: Copying to server...${NC}"
scp ewura-migration.tar.gz ${SERVER_USER}@${SERVER}:/tmp/
echo -e "${GREEN}✓ Files copied${NC}"
echo ""

echo -e "${YELLOW}Step 3: Setting up on server...${NC}"
ssh ${SERVER_USER}@${SERVER} << 'ENDSSH'
set -e

# Create directory if it doesn't exist
if [ ! -d "/var/lib/pgsql/scripts/ewura-migration" ]; then
    echo "Creating deployment directory..."
    mkdir -p /var/lib/pgsql/scripts/ewura-migration
fi

# Extract files
echo "Extracting files..."
cd /var/lib/pgsql/scripts/ewura-migration
tar -xzf /tmp/ewura-migration.tar.gz
rm /tmp/ewura-migration.tar.gz

# Set ownership to postgres
echo "Setting ownership to postgres..."
chown -R postgres:postgres /var/lib/pgsql/scripts/ewura-migration
chmod 755 /var/lib/pgsql/scripts/ewura-migration

echo "✓ Server setup complete"
ENDSSH
echo -e "${GREEN}✓ Server setup complete${NC}"
echo ""

echo -e "${YELLOW}Step 4: Setting up Python virtual environment...${NC}"
ssh ${SERVER_USER}@${SERVER} << 'ENDSSH'
set -e

sudo -i -u postgres bash << 'EOF'
cd /var/lib/pgsql/scripts/ewura-migration

# Create venv if it doesn't exist
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

# Activate and install dependencies
echo "Installing Python dependencies..."
source .venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet

echo "✓ Python environment ready"
EOF
ENDSSH
echo -e "${GREEN}✓ Python environment ready${NC}"
echo ""

echo -e "${YELLOW}Step 5: Checking environment configuration...${NC}"
ssh ${SERVER_USER}@${SERVER} << 'ENDSSH'
if [ ! -f "/var/lib/pgsql/scripts/ewura-migration/.env" ]; then
    echo "⚠️  WARNING: .env file not found!"
    echo "Creating template .env file..."
    sudo -i -u postgres bash << 'EOF'
cd /var/lib/pgsql/scripts/ewura-migration
cat > .env << 'ENVFILE'
# Production database connection
# Option 1: Use local postgres user (recommended if DB is on same server)
DATABASE_URL=postgresql+psycopg2://postgres:ewura%40123@localhost:5432/eservice_applications

# Option 2: Use remote database (if DB is on 10.1.8.166)
# DATABASE_URL=postgresql+psycopg2://appuser:ewura%40123@10.1.8.166:5432/eservice_applications
ENVFILE
chmod 600 .env
EOF
    echo "✓ Template .env created. Please edit with correct credentials."
else
    echo "✓ .env file exists"
fi
ENDSSH
echo ""

echo -e "${YELLOW}Step 6: Creating/updating systemd service...${NC}"
ssh ${SERVER_USER}@${SERVER} << 'ENDSSH'
cat > /etc/systemd/system/ewura-migration-api.service << 'EOF'
[Unit]
Description=EWURA Migration API Service
After=network.target postgresql.service

[Service]
Type=simple
User=postgres
Group=postgres
WorkingDirectory=/var/lib/pgsql/scripts/ewura-migration
Environment="PATH=/var/lib/pgsql/scripts/ewura-migration/.venv/bin"
ExecStart=/var/lib/pgsql/scripts/ewura-migration/.venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 4
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable ewura-migration-api
echo "✓ Systemd service configured"
ENDSSH
echo -e "${GREEN}✓ Service configured${NC}"
echo ""

echo -e "${YELLOW}Step 7: Restarting service...${NC}"
ssh ${SERVER_USER}@${SERVER} << 'ENDSSH'
if systemctl is-active --quiet ewura-migration-api; then
    echo "Restarting service..."
    systemctl restart ewura-migration-api
else
    echo "Starting service..."
    systemctl start ewura-migration-api
fi

sleep 2

# Check status
if systemctl is-active --quiet ewura-migration-api; then
    echo "✓ Service is running"
else
    echo "⚠️  Service failed to start. Check logs with: journalctl -u ewura-migration-api -xe"
    exit 1
fi
ENDSSH
echo -e "${GREEN}✓ Service started${NC}"
echo ""

# Clean up local package
rm -f ewura-migration.tar.gz

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "API is now running at: ${GREEN}http://10.1.8.144:8000${NC}"
echo -e "Swagger UI: ${GREEN}http://10.1.8.144:8000/docs${NC}"
echo ""
echo "Useful commands:"
echo "  View logs:    ssh ewura-admin@10.1.8.144 'journalctl -u ewura-migration-api -f'"
echo "  Check status: ssh ewura-admin@10.1.8.144 'systemctl status ewura-migration-api'"
echo "  Restart:      ssh ewura-admin@10.1.8.144 'systemctl restart ewura-migration-api'"
echo ""
