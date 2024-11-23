echo "Installing Docker and Docker Compose..."
curl -fsSL https://get.docker.com | sudo sh

echo "Check Docker installation..."
sudo docker --version
sudo docker compose version

echo "Installation complete.