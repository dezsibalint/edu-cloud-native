#!/bin/bash
# setup.sh - Full project setup from scratch

set -e

echo "OCR Microservices Full Setup"

# ===========================================
# PHASE 1: INFRASTRUCTURE DEPLOYMENT
# ===========================================

echo "PHASE 1/3: Deploying Infrastructure"

echo "[1/6] Deploying ConfigMap..."
kubectl apply -f infra/configmap.yaml
sleep 2

echo "[2/6] Deploying Zookeeper..."
kubectl apply -f infra/zookeeper.yaml
sleep 5

echo "[3/6] Deploying Kafka..."
kubectl apply -f infra/kafka.yaml
sleep 5

echo "[4/6] Deploying Redis..."
kubectl apply -f infra/redis.yaml
sleep 5

echo "[5/6] Deploying RabbitMQ..."
kubectl apply -f infra/rabbitmq.yaml
sleep 5

echo "[6/6] Deploying MinIO..."
kubectl apply -f infra/minio.yaml
sleep 5

echo "[7/7] Deploying Monitoring Stack..."
kubectl apply -f infra/monitoring/namespace.yaml
kubectl apply -f infra/monitoring/prometheus.yaml
kubectl apply -f infra/monitoring/node-exporter.yaml
kubectl apply -f infra/monitoring/grafana.yaml
sleep 5

echo "Infrastructure deployment complete"

# ===========================================
# PHASE 2: BUILD SERVICE IMAGES
# ===========================================

echo "PHASE 2/3: Building Service Images"

SERVICES=("filegrab" "pdf-to-image" "preprocessing" "ocr" "text-aggregation")

for service in "${SERVICES[@]}"; do
    echo "Building $service..."
    
    sudo docker build -t $service:latest services/$service/ || {
        echo "Failed to build $service"
        exit 1
    }
    
    echo "Exporting $service image..."
    sudo docker save $service:latest -o /tmp/$service.tar
    
    echo "Importing $service to K3s..."
    sudo k3s ctr images import /tmp/$service.tar
    
    sudo rm /tmp/$service.tar
    
    echo "$service image ready"
done

echo "All service images built and imported"

# ===========================================
# PHASE 3: DEPLOY SERVICES
# ===========================================

echo "PHASE 3/3: Deploying Services"

echo "[1/5] Deploying filegrab..."
kubectl apply -f services/filegrab/k8s/
sleep 3

echo "[2/5] Deploying pdf-to-image..."
kubectl apply -f services/pdf-to-image/k8s/
sleep 3

echo "[3/5] Deploying preprocessing..."
kubectl apply -f services/preprocessing/k8s/
sleep 3

echo "[4/5] Deploying ocr..."
kubectl apply -f services/ocr/k8s/
sleep 3

echo "[5/5] Deploying text-aggregation..."
kubectl apply -f services/text-aggregation/k8s/
sleep 5

echo "Setup Complete"

# ===========================================
# DISPLAY STATUS
# ===========================================

echo "Pod Status:"
kubectl get pods -o wide

echo ""
echo "Service Status:"
kubectl get services


echo ""
echo "Access URLs:"
echo "  FileGrab Upload:    http://<PUBLIC_IP>:30080/upload"
echo "  MinIO API:          http://<PUBLIC_IP>:30002"
echo "  MinIO Console:      http://<PUBLIC_IP>:30003"
echo "  RabbitMQ Management: http://<PUBLIC_IP>:30672"
echo "  Prometheus:         http://<PUBLIC_IP>:30900"
echo "  Grafana:            http://<PUBLIC_IP>:30300"

echo ""
echo "Credentials:"
echo "  MinIO:    minioadmin / minioadmin"
echo "  RabbitMQ: guest / guest"

echo ""
echo "Setup script completed successfully"