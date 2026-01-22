#!/bin/bash

# Interrompe lo script se un comando fallisce
set -e

# Definizione cartella dei manifest
MANIFEST_DIR="./k8s"

# Colori per i log
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${YELLOW}[1/7] Verifica Prerequisiti...${NC}"

if [ ! -d "$MANIFEST_DIR" ]; then
    echo -e "${RED}Errore: Cartella '$MANIFEST_DIR' non trovata!${NC}"
    exit 1
fi

if [ ! -f "kind-config.yaml" ]; then
    echo -e "${RED}Errore: File 'kind-config.yaml' non trovato nella root!${NC}"
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

# Controllo esistenza cluster (Lo manteniamo per risparmiare tempo di boot)
if kind get clusters | grep -q "migration-cluster"; then
    echo -e "${GREEN}Cluster 'migration-cluster' già attivo. Salto la creazione.${NC}"
else
    echo "Creazione nuovo cluster..."
    kind create cluster --config kind-config.yaml --name migration-cluster
fi

echo -e "${YELLOW}[3/7] Build & Force Load Immagini...${NC}"

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
    
    echo -e "${CYAN}--- Gestione $IMAGE_NAME ---${NC}"
    
    # 1. Build (Docker userà la cache dei layer se il codice non cambia, ma ricostruisce l'immagine)
    echo "Building..."
    docker build -t $IMAGE_NAME $BUILD_CONTEXT > /dev/null

    # 2. Load (Forziamo SEMPRE il caricamento in Kind)
    echo "Loading in Kind..."
    kind load docker-image $IMAGE_NAME --name migration-cluster
done

echo -e "${YELLOW}[4/7] Generazione Secret e Config...${NC}"

# 1. Carichiamo le variabili dal .env in memoria
set -a
. ./.env
set +a

# Pulizia vecchie configurazioni (Necessario se il cluster non viene ricreato)
kubectl delete configmap app-config --ignore-not-found
kubectl delete secret app-secrets --ignore-not-found

# --- FIX WINDOWS: Disabilita la conversione dei path per questo comando ---
export MSYS_NO_PATHCONV=1

# 2. Creiamo la configmap
echo "Creazione ConfigMap 'app-config'..."
kubectl create configmap app-config \
  --from-literal=KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP_SERVERS" \
  --from-literal=FLIGHTSDB_HOST="$FLIGHTSDB_HOST" \
  --from-literal=FLIGHTSDB_PORT="$FLIGHTSDB_PORT" \
  --from-literal=FLIGHTSDB_HOST_PORT="$FLIGHTSDB_HOST_PORT" \
  --from-literal=FLIGHTSDB_DATABASE="$FLIGHTSDB_DATABASE" \
  --from-literal=FLIGHTSDB_USER="$FLIGHTSDB_USER" \
  --from-literal=DATA_COLLECTOR_PORT="$DATA_COLLECTOR_PORT" \
  --from-literal=DATA_COLLECTOR_HOST_PORT="$DATA_COLLECTOR_HOST_PORT" \
  --from-literal=USER_DB="$USER_DB" \
  --from-literal=NAME_DB="$NAME_DB" \
  --from-literal=USER_DB_PORT="$USER_DB_PORT" \
  --from-literal=HOST_DB="$HOST_DB" \
  --from-literal=SMTP_SERVER="$SMTP_SERVER" \
  --from-literal=SMTP_PORT="$SMTP_PORT" \
  --from-literal=SENDER_EMAIL="$SENDER_EMAIL" \
  --from-literal=OPENSKY_SECRET_PATH="$OPENSKY_SECRET_PATH" \
  --from-literal=JWT_PRIVATEKEY_SECRET_PATH="$JWT_PRIVATEKEY_SECRET_PATH" \
  --from-literal=USER_MANAGER_PORT="$USER_MANAGER_PORT" \
  --from-literal=USER_MANAGER_HOST_PORT="$USER_MANAGER_HOST_PORT" \
  --from-literal=USER_DB_HOST_PORT="$USER_DB_HOST_PORT" \
  --from-literal=gRPC_HOST="$gRPC_HOST" \
  --from-literal=gRPC_HOST_PORT="$gRPC_HOST_PORT" \
  --from-literal=gRPC_PORT="$gRPC_PORT" \
  --from-literal=REDIS_PORT="$REDIS_PORT" \
  --from-literal=USER_REDIS_HOST="$USER_REDIS_HOST" \
  --from-literal=DATA_REDIS_HOST="$DATA_REDIS_HOST" \
  --from-literal=USER_REDIS_HOST_PORT="$USER_REDIS_HOST_PORT" \
  --from-literal=DATA_REDIS_HOST_PORT="$DATA_REDIS_HOST_PORT"

# 3. CREAZIONE SECRET
echo "Creazione Secret 'app-secrets'..."
kubectl create secret generic app-secrets \
  --from-literal=MARIADB_ROOT_PASSWORD="$PASSWORD_ROOT_DB" \
  --from-literal=FLIGHTSDB_ROOT_PASSWORD="$FLIGHTSDB_ROOT_PASSWORD" \
  --from-literal=FLIGHTSDB_PASSWORD="$FLIGHTSDB_PASSWORD" \
  --from-literal=PASSWORD_DB="$PASSWORD_DB" \
  --from-literal=EMAIL_PASSWORD="$EMAIL_PASSWORD" \
  --from-file=credentials.json=./data_collector/credentials.json \
  --from-file=private_key.pem=./user_manager/private_key.pem \
  --from-file=public_key.pem=./api_gateway/public_key.pem \
  --from-file=nginx.crt=./api_gateway/SSL/nginx.crt \
  --from-file=nginx.key=./api_gateway/SSL/nginx.key

# Riabilitiamo la conversione (opzionale, ma pulito)
unset MSYS_NO_PATHCONV

echo -e "${YELLOW}[5/7] Deployment Infrastruttura (Kafka & DB)...${NC}"
# Nota: Assicurati di avere imagePullPolicy: IfNotPresent nei manifest delle immagini esterne
kubectl apply -f ${MANIFEST_DIR}/kafka-cluster.yaml
kubectl apply -f ${MANIFEST_DIR}/user-db.yaml
kubectl apply -f ${MANIFEST_DIR}/flights-db.yaml
kubectl apply -f ${MANIFEST_DIR}/user-cache.yaml
kubectl apply -f ${MANIFEST_DIR}/data-cache.yaml

echo -e "${CYAN}Attesa avvio Infrastruttura (Kafka, DB, Redis)...${NC}"

# Attesa Kafka
echo "Waiting for Kafka..."
kubectl rollout status statefulset/kafka --timeout=300s

# Attesa DB
echo "Waiting for Flights DB..."
kubectl rollout status statefulset/flights-db --timeout=180s
echo "Waiting for User DB..."
kubectl rollout status statefulset/user-db --timeout=180s

# Attesa Cache
echo "Waiting for Redis Caches..."
kubectl wait --for=condition=available --timeout=60s deployment/user-cache
kubectl wait --for=condition=available --timeout=60s deployment/data-cache

# Verifica Readiness
echo -e "${CYAN}Verifica connettività servizi (Readiness Probes)...${NC}"
kubectl wait --for=condition=ready pod -l app=kafka --timeout=60s
kubectl wait --for=condition=ready pod -l app=flights-db --timeout=60s
kubectl wait --for=condition=ready pod -l app=user-db --timeout=60s

echo -e "${GREEN}>>> Infrastruttura PRONTA.${NC}"

echo -e "${YELLOW}[6/7] Inizializzazione Topic Kafka...${NC}"

# Pulizia job precedente per permettere il re-run
kubectl delete job kafka-init --ignore-not-found

kubectl apply -f ${MANIFEST_DIR}/kafka-init.yaml

echo "Waiting for Kafka Init Job..."
kubectl wait --for=condition=complete job/kafka-init --timeout=60s
echo -e "${GREEN}>>> Topic Kafka creati.${NC}"

echo -e "${YELLOW}[7/7] Deployment Applicazioni...${NC}"

# Riavvia i deployment per forzare l'uso della nuova immagine appena caricata
# Questo è utile se il deployment esisteva già
kubectl rollout restart deployment user-manager data-collector alert-system alert-notifier api-gateway 2>/dev/null || true

# Backend Core
kubectl apply -f ${MANIFEST_DIR}/user-manager.yaml
kubectl apply -f ${MANIFEST_DIR}/data-collector.yaml
kubectl wait --for=condition=available --timeout=120s deployment/user-manager
kubectl wait --for=condition=available --timeout=120s deployment/data-collector

# Consumers & UI
kubectl apply -f ${MANIFEST_DIR}/alert-system.yaml
kubectl apply -f ${MANIFEST_DIR}/alert-notifier-system.yaml
kubectl apply -f ${MANIFEST_DIR}/kafka-ui.yaml

# API Gateway
echo "Deploying API Gateway..."
kubectl apply -f ${MANIFEST_DIR}/api-gateway.yaml
kubectl wait --for=condition=available --timeout=60s deployment/api-gateway

echo -e "${GREEN}==================================================${NC}"
echo -e "${GREEN}   MIGRAZIONE COMPLETATA CON SUCCESSO! 🚀${NC}"
echo -e "${GREEN}==================================================${NC}"
echo -e "Accesso ai servizi:"
echo -e " - API Gateway HTTP:  http://localhost:80"
echo -e " - API Gateway HTTPS: https://localhost:443"
echo -e " - Kafka UI:          http://localhost:8080"