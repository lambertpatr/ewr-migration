#!/bin/bash
# EWURA Migration API Deployment Script
# Deploys to server 10.1.8.144:/var/lib/pgsql/scripts/ewura-migration

set -euo pipefail  # Exit on error, unset vars, or pipeline failure

# -------------------------- Configuration --------------------------
SERVER="10.1.8.144"
SERVER_USER="ewura-admin"
SERVER_PASS="secure@123"
DEPLOY_PATH="/var/lib/pgsql/scripts/ewura-migration"
LOCAL_PROJECT_PATH="/Users/lambert/Desktop/fast-api/ewura-migration"

# -------------------------- Color Helpers --------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# -------------------------- Prerequisites --------------------------
if ! command -v sshpass >/dev/null 2>&1; then
    echo -e "${RED}Error: sshpass is required but not installed.${NC}"
    echo -e "${YELLOW}Install via Homebrew: brew install sshpass${NC}"
    exit 1
fi

SCP_CMD=(sshpass -p "$SERVER_PASS" scp -o StrictHostKeyChecking=no -o PreferredAuthentications=password)
SSH_CMD=(sshpass -p "$SERVER_PASS" ssh -o StrictHostKeyChecking=no -o PreferredAuthentications=password)

# -------------------------- Banner --------------------------
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}EWURA Migration API Deployment${NC}"
echo -e "${GREEN}========================================${NC}\n"

# Ensure we run from project root
if [ "$(pwd)" != "$LOCAL_PROJECT_PATH" ]; then
    echo -e "${RED}Error: Must run from project root ($LOCAL_PROJECT_PATH)${NC}"
    exit 1
fi

# Step 1: Create deployment package
echo -e "${YELLOW}Step 1: Creating deployment package...${NC}"
COPYFILE_DISABLE=1 tar --no-xattrs --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    --exclude='.env*' \
    --exclude='*.tar.gz' \
    -czf ewura-migration.tar.gz .
echo -e "${GREEN}✓ Package created${NC}\n"

# Step 2: Copy package to server
echo -e "${YELLOW}Step 2: Copying to server...${NC}"
"${SCP_CMD[@]}" ewura-migration.tar.gz "${SERVER_USER}@${SERVER}:/tmp/"
echo -e "${GREEN}✓ Files copied${NC}\n"

# Step 3-7: Run remote installation and setup on server
echo -e "${YELLOW}Step 3: Running server-side setup tasks...${NC}"
"${SSH_CMD[@]}" "${SERVER_USER}@${SERVER}" bash -s <<EOF
set -e
PASS='${SERVER_PASS}'
DEPLOY_PATH='${DEPLOY_PATH}'

run_sudo() {
    printf '%s\n' "\$PASS" | sudo -S "\$@"
}

echo "1. Creating deployment directory (if needed)..."
if [ ! -d "\$DEPLOY_PATH" ]; then
    run_sudo mkdir -p "\$DEPLOY_PATH"
fi

echo "2. Extracting files..."
run_sudo tar -xzf /tmp/ewura-migration.tar.gz -C "\$DEPLOY_PATH"
run_sudo rm -f /tmp/ewura-migration.tar.gz

echo "3. Setting ownership to postgres..."
run_sudo chown -R postgres:postgres "\$DEPLOY_PATH"
run_sudo chmod 755 "\$DEPLOY_PATH"

echo "4. Setting up Python virtual environment..."
cat <<'EOS' > /tmp/setup-venv.sh
set -e
cd "${DEPLOY_PATH}"
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install --upgrade pip --quiet
pip install -r requirements.txt --quiet
EOS
run_sudo -u postgres bash /tmp/setup-venv.sh
rm -f /tmp/setup-venv.sh
echo "✓ Python environment ready"

echo "5. Checking environment configuration..."
ENV_FILE="${DEPLOY_PATH}/.env"
if [ ! -f "\$ENV_FILE" ]; then
    echo "Creating template .env..."
    cat <<'ENVFILE' > /tmp/ewura-env-temp
# Production database connection
DATABASE_URL=postgresql+psycopg2://postgres:ewura%40123@localhost:5432/eservice_applications
ENVFILE
    run_sudo cp /tmp/ewura-env-temp "\$ENV_FILE"
    run_sudo chmod 600 "\$ENV_FILE"
    run_sudo chown postgres:postgres "\$ENV_FILE"
    rm -f /tmp/ewura-env-temp
else
    echo "✓ .env file exists"
fi

echo "6. Creating/updating systemd service..."
cat <<'SERVICE' > /tmp/ewura-service-temp
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
SERVICE

run_sudo cp /tmp/ewura-service-temp /etc/systemd/system/ewura-migration-api.service
run_sudo chown root:root /etc/systemd/system/ewura-migration-api.service
run_sudo chmod 644 /etc/systemd/system/ewura-migration-api.service
rm -f /tmp/ewura-service-temp

run_sudo systemctl daemon-reload
run_sudo systemctl unmask ewura-migration-api
run_sudo systemctl enable ewura-migration-api

echo "7. Restarting service..."
if run_sudo systemctl is-active --quiet ewura-migration-api; then
    run_sudo systemctl restart ewura-migration-api
else
    run_sudo systemctl start ewura-migration-api
fi

sleep 2
if run_sudo systemctl is-active --quiet ewura-migration-api; then
    echo "✓ Service is running"
else
    echo "⚠️  Service failed to start. Check logs with: journalctl -u ewura-migration-api -xe"
    exit 1
fi
EOF
echo -e "${GREEN}✓ Server-side setup completed successfully!${NC}\n"

# Clean up local package
rm -f ewura-migration.tar.gz

# -------------------------- Summary --------------------------
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}\n"
echo -e "API:        ${GREEN}http://10.1.8.144:8000${NC}"
echo -e "Swagger UI: ${GREEN}http://10.1.8.144:8000/docs${NC}\n"
echo "Useful commands:"
echo "  View logs:    ssh ${SERVER_USER}@${SERVER} 'journalctl -u ewura-migration-api -f'"
echo "  Check status: ssh ${SERVER_USER}@${SERVER} 'systemctl status ewura-migration-api'"
echo "  Restart:      ssh ${SERVER_USER}@${SERVER} 'systemctl restart ewura-migration-api'"
echo