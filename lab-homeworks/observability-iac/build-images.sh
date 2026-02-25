#!/bin/bash
# build-images.sh - Build and import Docker images for specified microservice into K3s

set -e

SERVICE_NAME=$1

if [ -z "$SERVICE_NAME" ]; then
    echo "Usage: ./build-images.sh <service-name>"
    echo "Available services: filegrab, pdf-to-image, preprocessing, ocr, text-aggregation"
    exit 1
fi

echo "Rebuilding $SERVICE_NAME"

echo "[1/4] Building Docker image..."
sudo docker build -t $SERVICE_NAME:latest services/$SERVICE_NAME/

echo "[2/4] Exporting image..."
sudo docker save $SERVICE_NAME:latest -o /tmp/$SERVICE_NAME.tar

echo "[3/4] Importing to K3s..."
sudo k3s ctr images import /tmp/$SERVICE_NAME.tar

echo "[4/4] Cleaning up tar file..."
sudo rm /tmp/$SERVICE_NAME.tar

echo "Build complete"