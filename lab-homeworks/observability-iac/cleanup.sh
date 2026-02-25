#!/bin/bash
# cleanup.sh - Full project cleanup

set -e

echo "OCR Microservices Full Cleanup"

# ===========================================
# PHASE 1: DELETE KUBERNETES RESOURCES
# ===========================================

echo "PHASE 1/2: Cleaning Kubernetes Resources"

echo "[1/6] Deleting application services..."
kubectl delete -f services/text-aggregation/k8s/ --ignore-not-found=true
kubectl delete -f services/ocr/k8s/ --ignore-not-found=true
kubectl delete -f services/preprocessing/k8s/ --ignore-not-found=true
kubectl delete -f services/pdf-to-image/k8s/ --ignore-not-found=true
kubectl delete -f services/filegrab/k8s/ --ignore-not-found=true
sleep 30

echo "[2/6] Deleting MinIO..."
kubectl delete -f infra/minio.yaml --ignore-not-found=true
sleep 10

echo "[3/6] Deleting RabbitMQ..."
kubectl delete -f infra/rabbitmq.yaml --ignore-not-found=true
sleep 10

echo "[4/6] Deleting Redis..."
kubectl delete -f infra/redis.yaml --ignore-not-found=true
sleep 10

echo "[5/6] Deleting Kafka and Zookeeper..."
kubectl delete -f infra/kafka.yaml --ignore-not-found=true
kubectl delete -f infra/zookeeper.yaml --ignore-not-found=true
sleep 10

echo "[6/6] Deleting ConfigMap..."
kubectl delete -f infra/configmap.yaml --ignore-not-found=true
sleep 10

echo "Kubernetes cleanup complete"

# ===========================================
# PHASE 2: CLEAN DOCKER & K3S IMAGES
# ===========================================

echo "PHASE 2/2: Cleaning Docker and K3s Images"

echo "[1/2] Cleaning all Docker images..."
sudo docker image prune -a -f || echo "Docker prune failed"

echo "[2/2] Cleaning all K3s images..."
sudo k3s crictl rmi --prune || echo "K3s prune failed"

echo "Cleanup Complete"

# ===========================================
# DISPLAY FINAL STATUS
# ===========================================

echo "Remaining Kubernetes Resources:"
kubectl get all 2>/dev/null || echo "No resources found"

echo ""
echo "Remaining Docker Images:"
sudo docker images || echo "No Docker images"

echo ""
echo "Remaining K3s Images:"
sudo k3s crictl images || echo "No K3s images"

echo ""
echo "Cleanup script completed successfully"