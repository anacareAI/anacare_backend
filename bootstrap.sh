#!/bin/bash
# ============================================================
# bootstrap.sh — Run ON the DigitalOcean droplet
# ssh in, then: curl -s https://raw... | bash
# Or: copy this file up and run: bash bootstrap.sh
# ============================================================

set -e

echo "══════════════════════════════════════════"
echo "  AnaCare Pipeline — Server Bootstrap"
echo "══════════════════════════════════════════"

# 1. System updates
echo "▶ Updating system..."
apt-get update -qq && apt-get upgrade -y -qq

# 2. Install Node.js 20
echo "▶ Installing Node.js 20..."
curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
apt-get install -y nodejs

# 3. Install useful tools
echo "▶ Installing tools..."
apt-get install -y git htop screen jq pigz curl wget build-essential

# 4. Verify
node --version
npm --version

# 5. Create working directory
mkdir -p /opt/anacare-pipeline
cd /opt/anacare-pipeline

# 6. Set up swap (helps with large JSON parsing)
echo "▶ Setting up 8GB swap..."
if [ ! -f /swapfile ]; then
  fallocate -l 8G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

# 7. Tune system for large file processing
echo "▶ Tuning kernel params..."
cat >> /etc/sysctl.conf << 'EOF'
net.core.rmem_max = 134217728
net.core.wmem_max = 134217728
vm.swappiness = 10
EOF
sysctl -p

# 8. Install pm2 for process management
echo "▶ Installing pm2..."
npm install -g pm2

echo ""
echo "══════════════════════════════════════════"
echo "  Bootstrap complete!"
echo "  Node: $(node --version)"
echo "  npm:  $(npm --version)"
echo "  Swap: $(free -h | grep Swap)"
echo ""
echo "  Next steps:"
echo "  1. Upload pipeline files:  (run from your local machine)"
echo "     scp -i ~/.ssh/anacare_pipeline -r ./anacare-pipeline/* root@SERVER_IP:/opt/anacare-pipeline/"
echo ""
echo "  2. SSH back in and run:"
echo "     cd /opt/anacare-pipeline && npm install"
echo "     npm run migrate"
echo "     npm run full-pipeline"
echo "══════════════════════════════════════════"
