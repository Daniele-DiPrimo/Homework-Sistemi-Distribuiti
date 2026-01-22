#!/bin/bash

# Interrompe lo script se un comando fallisce
set -e

# Definizione cartella dei manifest
MANIFEST_DIR="./k8s"

# Colori per i log
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}[1/7] Verifica Prerequisiti...${NC}"

if [ ! -d "$MANIFEST_DIR" ]; then
    echo -e "${RED}Errore: Cartella '$MANIFEST_DIR' non trovata!${NC}"
    exit 1
fi

if [ ! -f "00-kind-config.yaml" ]; then
    echo -e "${RED}Errore: File '00-kind-config.yaml' non trovato nella root!${NC}"
    exit 1
fi

if [ ! -f .env ]; then
    echo -e "${RED}Errore: File .env non trovato nella root!${NC}"
    exit 1
fi

if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Errore: Docker non è in esecuzione.${NC}"
    exit 1
fi

echo -e "${YELLOW}[2/7] Gestione Cluster Kind...${NC}"

if kind get clusters | grep -q "migration-cluster"; then
    echo "Eliminazione cluster esistente..."
    kind delete cluster --name migration-cluster
fi

echo "Creazione nuovo cluster..."
kind create cluster --config 00-kind-config.yaml --name migration-cluster

echo -e "${YELLOW}[3/7] Building delle Immagini Docker...${NC}"
declare -a services=(
    "local/api-gateway:v1 ./api_gateway"
    "local/user-manager:v1 ./user_manager"
    "local/data-collector:v1 ./data_collector"
    "local/alert-system:v1 ./alert_system"
    "local/alert-notifier:v1 ./alert_notifier_system"
)

for service in "${services[@]}"; do
    set -- $service
    IMAGE_NAME=$1
    BUILD_CONTEXT=$2
    
    echo "Building $IMAGE_NAME da $BUILD_CONTEXT..."
    docker build -t $IMAGE_NAME $BUILD_CONTEXT
done

echo -e "${YELLOW}[4/7] Caricamento Immagini in Kind...${NC}"
for service in "${services[@]}"; do
    set -- $service
    IMAGE_NAME=$1
    echo "Loading $IMAGE_NAME..."
    kind load docker-image $IMAGE_NAME --name migration-cluster
done

echo -e "${YELLOW}[5/7] Generazione Secret e Config...${NC}"

# Applica il ConfigMap
kubectl apply -f ${MANIFEST_DIR}/01-config-secrets.yaml

# --- FIX PER IL SECRET ---
# 1. Carichiamo le variabili dal .env in memoria
set -a
. ./.env
set +a

# 2. Creiamo il secret combinando --from-literal (variabili) e --from-file (files)
# Nota: Usiamo le virgolette "$VAR" per gestire eventuali spazi nelle password
kubectl create secret generic app-secrets \
  --from-literal=MARIADB_ROOT_PASSWORD="$PASSWORD_ROOT_DB" \
  --from-literal=FLIGHTSDB_ROOT_PASSWORD="$FLIGHTSDB_ROOT_PASSWORD" \
  --from-literal=FLIGHTSDB_USER="$FLIGHTSDB_USER" \
  --from-literal=FLIGHTSDB_PASSWORD="$FLIGHTSDB_PASSWORD" \
  --from-literal=FLIGHTSDB_DATABASE="$FLIGHTSDB_DATABASE" \
  --from-literal=FLIGHTSDB_PORT="$FLIGHTSDB_PORT" \
  --from-literal=FLIGHTSDB_HOST="$FLIGHTSDB_HOST" \
  --from-literal=FLIGHTSDB_HOST_PORT="$FLIGHTSDB_HOST_PORT" \
  --from-literal=DATA_COLLECTOR_PORT="$DATA_COLLECTOR_PORT" \
  --from-literal=DATA_COLLECTOR_HOST_PORT="$DATA_COLLECTOR_HOST_PORT" \
  --from-literal=USER_DB="$USER_DB" \
  --from-literal=PASSWORD_DB="$PASSWORD_DB" \
  --from-literal=NAME_DB="$NAME_DB" \
  --from-literal=USER_DB_PORT="$USER_DB_PORT" \
  --from-literal=HOST_DB="$HOST_DB" \
  --from-literal=SMTP_SERVER="$SMTP_SERVER" \
  --from-literal=SMTP_PORT="$SMTP_PORT" \
  --from-literal=SENDER_EMAIL="$SENDER_EMAIL" \
  --from-literal=EMAIL_PASSWORD="$EMAIL_PASSWORD" \
  --from-literal=OPENSKY_SECRET_PATH="$OPENSKY_SECRET_PATH" \
  --from-literal=JWT_PRIVATEKEY_SECRET_PATH="$JWT_PRIVATEKEY_SECRET_PATH" \
  --from-literal=USER_MANAGER_PORT="$USER_MANAGER_PORT" \
  --from-literal=USER_MANAGER_HOST_PORT="$USER_MANAGER_HOST_PORT" \
  --from-literal=USER_DB_HOST_PORT="$USER_DB_HOST_PORT" \
  --from-literal=gRPC_HOST_PORT="$gRPC_HOST_PORT" \
  --from-literal=gRPC_PORT="$gRPC_PORT" \
  --from-literal=REDIS_PORT="$REDIS_PORT" \
  --from-literal=USER_REDIS_HOST="$USER_REDIS_HOST" \
  --from-literal=DATA_REDIS_HOST="$DATA_REDIS_HOST" \
  --from-literal=USER_REDIS_HOST_PORT="$USER_REDIS_HOST_PORT" \
  --from-literal=DATA_REDIS_HOST_PORT="$DATA_REDIS_HOST_PORT" \
  --from-file=credentials.json=./data_collector/credentials.json \
  --from-file=private_key.pem=./user_manager/private_key.pem \
  --from-file=public_key.pem=./api_gateway/public_key.pem \
  --from-file=nginx.crt=./api_gateway/SSL/nginx.crt \
  --from-file=nginx.key=./api_gateway/SSL/nginx.key

echo "Secret 'app-secrets' creato con successo."

echo -e "${YELLOW}[6/7] Deployment Infrastruttura (Kafka & DB)...${NC}"
kubectl apply -f ${MANIFEST_DIR}/02-kafka-cluster.yaml
kubectl apply -f ${MANIFEST_DIR}/03-data-layer.yaml

echo "Attesa inizializzazione Database e Kafka (30 secondi)..."
sleep 30

echo -e "${YELLOW}[7/7] Deployment Applicazioni...${NC}"
kubectl apply -f ${MANIFEST_DIR}/04-backend-apps.yaml
kubectl apply -f ${MANIFEST_DIR}/05-frontend-gateway.yaml

# Lancia il job di init per ultimo
kubectl apply -f ${MANIFEST_DIR}/06-kafka-init.yaml

echo -e "${GREEN}==================================================${NC}"
echo -e "${GREEN}   MIGRAZIONE COMPLETATA CON SUCCESSO! 🚀${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "Accesso ai servizi:"
echo -e " - API Gateway HTTP:  http://localhost:80"
echo -e " - API Gateway HTTPS: https://localhost:443"
echo -e " - Kafka UI:          http://localhost:8080"