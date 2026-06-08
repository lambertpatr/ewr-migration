# Quick Deploy Guide — EWURA Migration API

## Prerequisites Checklist
- [ ] Server 10.1.8.144 is accessible
- [ ] You can SSH as ewura-admin (password: secure@123)
- [ ] PostgreSQL is installed on the server
- [ ] Database `eservice_applications` exists

---

## Option 1: Automated Deployment (Recommended)

Run the automated deployment script from your Mac:

```bash
cd /Users/lambert/Desktop/fast-api/ewura-migration

# Run the deployment script
./deploy.sh
```

The script will:
1. ✅ Package the application
2. ✅ Copy files to server
3. ✅ Set up Python environment
4. ✅ Create systemd service
5. ✅ Start the API

**Access the API:** http://10.1.8.144:8000/docs

---

## Option 2: Manual Step-by-Step

### Step 1: Test SSH Connection
```bash
ssh ewura-admin@10.1.8.144
# Enter password: secure@123
```

### Step 2: Prepare Server
```bash
# On the server as ewura-admin
sudo mkdir -p /var/lib/pgsql/scripts/ewura-migration
sudo chown -R postgres:postgres /var/lib/pgsql/scripts/ewura-migration
sudo chmod 755 /var/lib/pgsql/scripts/ewura-migration
```

### Step 3: Copy Files
```bash
# On your Mac
cd /Users/lambert/Desktop/fast-api/ewura-migration
tar --exclude='.venv' --exclude='__pycache__' --exclude='*.pyc' --exclude='.git' --exclude='.env' -czf ewura-migration.tar.gz .
scp ewura-migration.tar.gz ewura-admin@10.1.8.144:/tmp/
```

### Step 4: Extract and Set Permissions
```bash
# On the server
ssh ewura-admin@10.1.8.144

cd /var/lib/pgsql/scripts/ewura-migration
sudo -u postgres tar -xzf /tmp/ewura-migration.tar.gz
sudo rm /tmp/ewura-migration.tar.gz
```

### Step 5: Install Dependencies
```bash
# Switch to postgres user
sudo -i -u postgres
cd /var/lib/pgsql/scripts/ewura-migration

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install packages
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 6: Configure Database
```bash
# Still as postgres user
cat > .env << 'EOF'
DATABASE_URL=postgresql+psycopg2://postgres:ewura%40123@localhost:5432/eservice_applications
EOF

chmod 600 .env
```

### Step 7: Test the Application
```bash
# Test run (Ctrl+C to stop)
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Open in browser: http://10.1.8.144:8000/docs

### Step 8: Create Systemd Service
```bash
# Exit postgres user (Ctrl+D)
# Back as ewura-admin

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

sudo systemctl daemon-reload
sudo systemctl enable ewura-migration-api
sudo systemctl start ewura-migration-api
```

### Step 9: Verify
```bash
# Check service status
sudo systemctl status ewura-migration-api

# View logs
sudo journalctl -u ewura-migration-api -f
```

---

## Testing Database Connection

Before deploying, verify database access:

```bash
# On the server
sudo -i -u postgres

# Test local connection
psql -h localhost -d eservice_applications -c "SELECT version();"
# Password: ewura@123

# List tables
psql -h localhost -d eservice_applications -c "\dt"
```

---

## Firewall Configuration

If the API is not accessible from your browser:

```bash
# On the server
sudo firewall-cmd --permanent --add-port=8000/tcp
sudo firewall-cmd --reload

# Verify
sudo firewall-cmd --list-ports
```

---

## Common Commands

```bash
# Start service
sudo systemctl start ewura-migration-api

# Stop service
sudo systemctl stop ewura-migration-api

# Restart service
sudo systemctl restart ewura-migration-api

# View logs (live)
sudo journalctl -u ewura-migration-api -f

# Check status
sudo systemctl status ewura-migration-api
```

---

## Verification Checklist

After deployment, verify:

- [ ] API responds: `curl http://10.1.8.144:8000/health` (or /docs)
- [ ] Service is running: `systemctl status ewura-migration-api`
- [ ] No errors in logs: `journalctl -u ewura-migration-api -n 50`
- [ ] Swagger UI loads: http://10.1.8.144:8000/docs
- [ ] Database connection works (test an endpoint)

---

## Troubleshooting

### Service won't start
```bash
# Check logs
sudo journalctl -u ewura-migration-api -xe

# Try manual start to see errors
sudo -i -u postgres
cd /var/lib/pgsql/scripts/ewura-migration
source .venv/bin/activate
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### Database connection fails
```bash
# Verify postgres password
sudo -i -u postgres
psql -h localhost -d eservice_applications
# Password: ewura@123

# Check .env file
cat /var/lib/pgsql/scripts/ewura-migration/.env
```

### Port 8000 already in use
```bash
# Find what's using it
sudo netstat -tlnp | grep 8000

# Change port in service file
sudo nano /etc/systemd/system/ewura-migration-api.service
# Change --port 8000 to --port 8001
sudo systemctl daemon-reload
sudo systemctl restart ewura-migration-api
```

---

## Need Help?

**Server Details:**
- IP: 10.1.8.144
- Admin User: ewura-admin
- Admin Password: secure@123
- Postgres Password: ewura@123
- Deploy Path: /var/lib/pgsql/scripts/ewura-migration
- API URL: http://10.1.8.144:8000/docs
