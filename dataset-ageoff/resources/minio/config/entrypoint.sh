#!/bin/bash
set -m

# Start opensearch in background
/usr/bin/docker-entrypoint.sh minio server /data --console-address :9001 &

# Execute init scripts
for FILE in /docker-entrypoint-init.d/*.sh; do
  bash "${FILE}" 
done

# Move opensearch to foreground
fg %1
