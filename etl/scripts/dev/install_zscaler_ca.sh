#!/bin/bash

# 1. Export the Zscaler cert from the macOS Keychain to a temp file
security find-certificate -c "Zscaler Root CA" -p > /tmp/zscaler.pem

# 2. Check if the certificate was successfully found
if [ ! -s /tmp/zscaler.pem ]; then
    echo "Error: Zscaler Root CA certificate not found in Mac Keychain."
    exit 1
fi

# 3. Stream the file into the Podman VM and update the trust store
echo "Injecting certificate into Podman VM..."
cat /tmp/zscaler.pem | podman machine ssh "sudo tee /etc/pki/ca-trust/source/anchors/zscaler.crt > /dev/null && sudo update-ca-trust"

# 4. Restart the machine to apply changes
echo "Restarting Podman machine..."
podman machine stop
podman machine start

echo "Done! Podman should now trust your Zscaler proxy."
