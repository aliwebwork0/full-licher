#!/bin/sh
set -e

mkdir -p /root/.config/rclone

if [ -z "$RCLONE_CONF" ]; then
    echo "ERROR: RCLONE_CONF environment variable is not set"
    exit 1
fi

printf "%b" "$RCLONE_CONF" > /root/.config/rclone/rclone.conf
chmod 600 /root/.config/rclone/rclone.conf

export RCLONE_CONFIG=/root/.config/rclone/rclone.conf

echo "==> rclone config written successfully"
echo "==> Starting gunicorn..."
exec gunicorn -b 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 app:app
