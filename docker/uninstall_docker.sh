#!/usr/bin/env bash

set -e

echo "Uninstalling Docker..."

# Remove Docker Engine, CLI, containerd, and Docker Compose
sudo apt-get purge -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Remove all residual configuration files, images, containers, volumes
sudo rm -rf /var/lib/docker
sudo rm -rf /var/lib/containerd

# Remove Docker Compose binary if it was installed separately
sudo rm -f /usr/local/bin/docker-compose

echo "Docker uninstalled successfully!"
