#!/usr/bin/env python3
import subprocess
import paramiko
import time

SERVER = "10.1.8.144"
USER = "ewura-admin"
PASS = "secure@123"

def main():
    print("Creating fresh deployment package /tmp/ewura-migration-upload.tar.gz...")
    subprocess.run(
        "COPYFILE_DISABLE=1 tar --no-xattrs --exclude='.venv' "
        "--exclude='__pycache__' --exclude='*.pyc' --exclude='.git' "
        "--exclude='*.tar.gz' -czf /tmp/ewura-migration-upload.tar.gz .",
        shell=True,
        check=True
    )
    print("✓ Package created.")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"Connecting to {SERVER}...")
    ssh.connect(SERVER, username=USER, password=PASS)

    sftp = ssh.open_sftp()
    print("Uploading ewura-migration.tar.gz...")
    sftp.put("/tmp/ewura-migration-upload.tar.gz", "/tmp/ewura-migration.tar.gz")
    sftp.close()
    try:
        import os
        os.remove("/tmp/ewura-migration-upload.tar.gz")
    except Exception:
        pass

    commands = [
        "echo 'secure@123' | sudo -S mkdir -p /var/lib/pgsql/scripts/ewura-migration",
        "echo 'secure@123' | sudo -S tar -xzf /tmp/ewura-migration.tar.gz -C /var/lib/pgsql/scripts/ewura-migration",
        "echo 'secure@123' | sudo -S rm -f /tmp/ewura-migration.tar.gz",
        "echo 'secure@123' | sudo -S chown -R postgres:postgres /var/lib/pgsql/scripts/ewura-migration",
        "echo 'secure@123' | sudo -S chmod 755 /var/lib/pgsql/scripts/ewura-migration",
        "echo 'secure@123' | sudo -S -u postgres bash -c 'cd /var/lib/pgsql/scripts/ewura-migration && source .venv/bin/activate && pip install -r requirements.txt --quiet'",
        "echo 'secure@123' | sudo -S systemctl daemon-reload",
        "echo 'secure@123' | sudo -S systemctl restart ewura-migration-api",
        "echo 'secure@123' | sudo -S systemctl is-active ewura-migration-api"
    ]

    for cmd in commands:
        print(f"Running: {cmd.split('|')[1].strip() if '|' in cmd else cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, get_pty=True)
        out = stdout.read().decode('utf-8').strip()
        err = stderr.read().decode('utf-8').strip()
        if out:
            print("  Output:", out)
        if err:
            print("  Error:", err)

    ssh.close()
    print("Deployment to 10.1.8.144 complete!")

if __name__ == "__main__":
    main()
