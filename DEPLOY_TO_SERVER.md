# Deploy EWURA Migration API to Server 10.1.8.144

## Overview
This guide deploys the FastAPI migration service to `/var/lib/pgsql/scripts/ewura-migration` on server 10.1.8.144 with postgres user ownership.

---

## Step 1: Prepare the Server

SSH into the server as administrator:

```bash
ssh ewura-admin@10.1.8.144
# Password: secure@123
```

### Create deployment directory

```bash
# Create the directory
sudo mkdir -p /var/lib/pgsql/scripts/ewura-migration

# Set postgres as owner
sudo chown -R postgres:postgres /var/lib/pgsql/scripts/ewura-migration

# Set proper permissions
sudo chmod 755 /var/lib/pgsql/scripts/ewura-migration
```

---

## Step 2: Install System Dependencies

```bash
# Install Python 3 and pip if not already installed
sudo yum install -y python3 python3-pip python3-devel postgresql-devel gcc

# Verify installation
python3 --version
pip3 --version
```

---

## Step 3: Copy Files to Server

From your local machine (Mac), copy the project files:

```bash
# Navigate to the project directory
cd /Users/lambert/Desktop/fast-api/ewura-migration

# Create a deployment package (excludes .venv, __pycache__, etc.)
tar --exclude='.venv' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.git' \
    --exclude='.env' \
    -czf ewura-migration.tar.gz .

# Copy to server
scp ewura-migration.tar.gz root@10.1.8.144:/tmp/

# SSH to server and extract
ssh root@10.1.8.144 << 'EOF'
cd /var/lib/pgsql/scripts/ewura-migration
sudo -u postgres tar -xzf /tmp/ewura-migration.tar.gz
sudo rm /tmp/ewura-migration.tar.gz
EOF
```

---

## Step 4: Set Up Python Virtual Environment

SSH to the server and run as postgres user:

```bash
ssh root@10.1.8.144

# Switch to postgres user
sudo -i -u postgres

# Navigate to project directory
cd /var/lib/pgsql/scripts/ewura-migration

# Create virtual environment
python3 -m venv .venv

# Activate virtual environment
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip

# Install dependencies
pip install -r requirements.txt
```

---

## Step 5: Configure Environment Variables

Create the production `.env` file:

```bash
# Still as postgres user
cd /var/lib/pgsql/scripts/ewura-migration

# Create .env file
cat > .env << 'EOF'
# Production database connection
# Option 1: Use local postgres user (recommended if DB is on same server)
DATABASE_URL=postgresql+psycopg2://postgres:ewura%40123@localhost:5432/eservice_applications

# Option 2: Use remote database (if DB is on 10.1.8.166)
# DATABASE_URL=postgresql+psycopg2://appuser:ewura%40123@10.1.8.166:5432/eservice_applications
EOF

# Secure the .env file
chmod 600 .env
```

**Note:** Postgres password is `ewura@123` (URL-encoded as `ewura%40123`)

---

## Step 6: Test the Application

```bash
# As postgres user with venv activated
cd /var/lib/pgsql/scripts/ewura-migration
source .venv/bin/activate

# Test run
uvicorn app.main:app --host 0.0.0.0 --port 8000

# Open in browser: http://10.1.8.144:8000/docs
# Press Ctrl+C to stop
```

---

## Step 7: Create Systemd Service (Production Setup)

Create a systemd service to run the API automatically:

```bash
# As root, create service file
sudo tee /etc/systemd/system/ewura-migration-api.service > /dev/null << 'EOF'
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

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
sudo systemctl daemon-reload

# Enable service to start on boot
sudo systemctl enable ewura-migration-api

# Start the service
sudo systemctl start ewura-migration-api

# Check status
sudo systemctl status ewura-migration-api
```

---

## Step 8: Configure Firewall (if needed)

```bash
# Allow port 8000 through firewall
sudo firewall-cmd --permanent --add-port=8000/tcp
sudo firewall-cmd --reload

# Or if using iptables
sudo iptables -A INPUT -p tcp --dport 8000 -j ACCEPT
sudo service iptables save
```

---

## Step 9: Verify Deployment

```bash
# Check if service is running
sudo systemctl status ewura-migration-api

# View logs
sudo journalctl -u ewura-migration-api -f

# Test API endpoint
curl http://10.1.8.144:8000/docs
curl http://10.1.8.144:8000/health  # if you have a health endpoint
```

Access Swagger UI from your browser: **http://10.1.8.144:8000/docs**

---

## Management Commands

```bash
# Start service
sudo systemctl start ewura-migration-api

# Stop service
sudo systemctl stop ewura-migration-api

# Restart service
sudo systemctl restart ewura-migration-api

# View logs (live)
sudo journalctl -u ewura-migration-api -f

# View last 100 lines of logs
sudo journalctl -u ewura-migration-api -n 100
```

---

## Step 10: Set Up Log Rotation (Optional)

```bash
# Create logrotate config
sudo tee /etc/logrotate.d/ewura-migration-api > /dev/null << 'EOF'
/var/log/ewura-migration/*.log {
    daily
    rotate 14
    compress
    delaycompress
    notifempty
    create 0640 postgres postgres
    sharedscripts
    postrotate
        systemctl reload ewura-migration-api > /dev/null 2>&1 || true
    endscript
}
EOF
```

---

## Updating the Application

When you need to update the code:

```bash
# From your local machine
cd /Users/lambert/Desktop/fast-api/ewura-migration
tar --exclude='.venv' --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' -czf ewura-migration.tar.gz .
scp ewura-migration.tar.gz ewura-admin@10.1.8.144:/tmp/

# On the server as ewura-admin
sudo systemctl stop ewura-migration-api
cd /var/lib/pgsql/scripts/ewura-migration
sudo -u postgres tar -xzf /tmp/ewura-migration.tar.gz
sudo -u postgres bash -c "source .venv/bin/activate && pip install -r requirements.txt"
sudo systemctl start ewura-migration-api
sudo rm /tmp/ewura-migration.tar.gz
```

---

## Troubleshooting

### Permission Issues
```bash
# Fix ownership recursively
sudo chown -R postgres:postgres /var/lib/pgsql/scripts/ewura-migration

# Fix permissions
sudo chmod -R 755 /var/lib/pgsql/scripts/ewura-migration
sudo chmod 600 /var/lib/pgsql/scripts/ewura-migration/.env
```

### Database Connection Issues
```bash
# Test database connection as postgres user (password: ewura@123)
sudo -i -u postgres
psql -h localhost -U postgres -d eservice_applications -c "SELECT version();"
# When prompted, enter: ewura@123

# Or test with the app user
psql -h 10.1.8.166 -U appuser -d eservice_applications -c "SELECT version();"
```

### Service Won't Start
```bash
# Check detailed logs
sudo journalctl -u ewura-migration-api -xe

# Test manually
sudo -i -u postgres
cd /var/lib/pgsql/scripts/ewura-migration
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Port Already in Use
```bash
# Find what's using port 8000
sudo netstat -tlnp | grep 8000
# or
sudo lsof -i :8000

# Change port in systemd service file
sudo nano /etc/systemd/system/ewura-migration-api.service
# Change --port 8000 to --port 8001 (or another free port)
sudo systemctl daemon-reload
sudo systemctl restart ewura-migration-api
```

---

## Security Recommendations

1. **Use a non-root database user** instead of postgres for the application
2. **Set up SSL/TLS** if exposing the API to the internet
3. **Use nginx reverse proxy** for better security and performance
4. **Restrict firewall rules** to only allow trusted IPs
5. **Regular backups** of the database before running migrations
6. **Keep .env file secure** with 600 permissions

---

## Quick Reference

| Item | Value |
|---|---|
| **Server** | 10.1.8.144 |
| **Admin User** | ewura-admin (password: secure@123) |
| **Postgres Password** | ewura@123 |
| **Install Path** | /var/lib/pgsql/scripts/ewura-migration |
| **Owner** | postgres:postgres |
| **Service Name** | ewura-migration-api |
| **API Port** | 8000 |
| **Swagger UI** | http://10.1.8.144:8000/docs |
| **Logs** | `sudo journalctl -u ewura-migration-api -f` |
