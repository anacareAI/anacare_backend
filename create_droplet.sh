#!/bin/bash
# ============================================================
# AnaCare VPS Setup — DigitalOcean Droplet
# Run this on YOUR LOCAL MACHINE (not the droplet)
# Prerequisites: doctl CLI installed and authenticated
#   brew install doctl  (Mac)
#   sudo snap install doctl  (Linux)
#   doctl auth init  (paste your DO API token)
# ============================================================

set -e

DROPLET_NAME="anacare-pipeline"
REGION="nyc3"           # Close to Neon us-east-1
SIZE="s-4vcpu-8gb"      # 4 CPU, 8GB RAM, 160GB SSD — $48/mo. Delete when done.
IMAGE="ubuntu-24-04-x64"
SSH_KEY_NAME="anacare-pipeline-key"

echo "══════════════════════════════════════════"
echo "  AnaCare Pipeline — DigitalOcean Setup"
echo "══════════════════════════════════════════"

# 1. Generate SSH key
echo "▶ Generating SSH key..."
ssh-keygen -t ed25519 -f ~/.ssh/anacare_pipeline -N "" -C "anacare-pipeline" 2>/dev/null || true
PUB_KEY=$(cat ~/.ssh/anacare_pipeline.pub)

# 2. Upload SSH key to DO
echo "▶ Uploading SSH key to DigitalOcean..."
KEY_ID=$(doctl compute ssh-key import "$SSH_KEY_NAME" \
  --public-key-file ~/.ssh/anacare_pipeline.pub \
  --format ID --no-header 2>/dev/null || \
  doctl compute ssh-key list --format ID,Name --no-header | grep "$SSH_KEY_NAME" | awk '{print $1}')

# 3. Create droplet
echo "▶ Creating droplet ($SIZE in $REGION)..."
DROPLET_ID=$(doctl compute droplet create "$DROPLET_NAME" \
  --region "$REGION" \
  --size "$SIZE" \
  --image "$IMAGE" \
  --ssh-keys "$KEY_ID" \
  --wait \
  --format ID --no-header)

echo "▶ Droplet created: $DROPLET_ID"

# 4. Get IP
sleep 10
DROPLET_IP=$(doctl compute droplet get "$DROPLET_ID" --format PublicIPv4 --no-header)
echo "▶ Droplet IP: $DROPLET_IP"

# 5. Wait for SSH to be ready
echo "▶ Waiting for SSH..."
until ssh -i ~/.ssh/anacare_pipeline -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
  root@$DROPLET_IP "echo ready" 2>/dev/null; do
  sleep 5; echo "  ...waiting"
done

echo ""
echo "══════════════════════════════════════════"
echo "  Droplet ready!"
echo "  IP: $DROPLET_IP"
echo ""
echo "  SSH in with:"
echo "  ssh -i ~/.ssh/anacare_pipeline root@$DROPLET_IP"
echo ""
echo "  Next: run bootstrap.sh on the droplet"
echo "══════════════════════════════════════════"

# Save IP for other scripts
echo "$DROPLET_IP" > .droplet_ip
echo "IP saved to .droplet_ip"
