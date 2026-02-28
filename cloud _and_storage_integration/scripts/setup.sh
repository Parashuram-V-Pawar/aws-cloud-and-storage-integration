#!/bin/bash
echo "Updating system..."
sudo apt-get update -y
sudo apt-get upgrade -y

echo "Installing Python3 pip..."
sudo apt-get install python3-pip -y

echo "Installing boto3..."
pip3 install boto3

echo "Creating data and download directories..."
mkdir -p data
mkdir -p download

echo "Setup complete. Place your CSV file in data/ folder."