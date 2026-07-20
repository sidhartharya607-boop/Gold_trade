#!/bin/bash
echo "==============================================="
echo "   AWS UBUNTU SERVER ARBITRAGE SETUP SCRIPT   "
echo "==============================================="
echo

# Update system packages
echo "[*] Updating apt packages..."
sudo apt update && sudo apt upgrade -y

# Install Python, Venv, Git, Node, and NPM
echo "[*] Installing Python, Git, Node.js and NPM..."
sudo apt install python3 python3-pip python3-venv git nodejs npm -y

# Install PM2 globally
echo "[*] Installing PM2 Process Manager..."
sudo npm install -g pm2

# Create Python virtual environment and install requirements
echo "[*] Setting up Python virtual environment..."
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

# Create local environment config
if [ ! -f .env ]; then
    echo "[*] Creating .env file..."
    echo "AUTH_TOKEN=secret_arbitrage_token_2026" >> .env
    echo "HOST=0.0.0.0" >> .env
    echo "PORT=7890" >> .env
fi

# Start with PM2 using virtual environment interpreter
echo "[*] Starting terminal process under PM2..."
pm2 start main.py --name "gold-terminal" --interpreter ./venv/bin/python

# Save PM2 state for startup boot persistence
pm2 save
pm2 startup

echo
echo "==============================================="
echo "   DEPLOYMENT SETUP COMPLETED SUCCESSFULLY!    "
echo "==============================================="
echo "Please copy-paste the 'sudo env PATH...' command shown by PM2 above to lock in autostart."
echo "Ensure Port 7890 is open in your AWS EC2 Security Groups."
echo "You can access your terminal at: http://YOUR_AWS_INSTANCE_IP:7890/"
echo "==============================================="
