#!/bin/bash

echo "Installing FFmpeg and other dependencies..."

# Detect package manager
if command -v apt-get &> /dev/null; then
    # Debian/Ubuntu
    sudo apt-get update
    sudo apt-get install -y ffmpeg python3 python3-pip
elif command -v dnf &> /dev/null; then
    # Fedora
    sudo dnf install -y ffmpeg python3 python3-pip
elif command -v pacman &> /dev/null; then
    # Arch Linux
    sudo pacman -Sy ffmpeg python3 python3-pip
elif command -v yum &> /dev/null; then
    # CentOS/RHEL
    sudo yum install -y epel-release
    sudo yum install -y ffmpeg python3 python3-pip
else
    echo "Could not detect package manager. Please install FFmpeg manually."
    exit 1
fi

# Install Python dependencies
echo "Installing Python dependencies..."
pip3 install -r requirements.txt

echo "Setup completed successfully!"
echo "Make sure to configure your .env file with the Bot Token" 