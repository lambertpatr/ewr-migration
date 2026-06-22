sshpass -p 'secure@123' ssh -o StrictHostKeyChecking=no ewura-admin@10.1.8.144 << 'INNER_EOF'
echo "secure@123" | sudo -S -u postgres psql -d eservice_applications -f /var/lib/pgsql/scripts/ewura-migration/app/migrations/backfill_certificate_owner.sql
INNER_EOF
