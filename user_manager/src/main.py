"""User Manager microservice

Espone API REST per registrare/eliminare utenti e un server gRPC per
verificare l'esistenza di un utente (usato da altri servizi).
"""

from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import grpc
import sys
import os
import json
import redis
import logging
from extensions import db
from user import User
import jwt
import uuid
from prometheus_client import start_http_server, Counter

# --- FILTRO LOG INTELLIGENTE ---
class HealthCheckFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()
        
        # Se è una chiamata a /health...
        if '/health' in msg:
            # ...e il codice è 200 (Successo), ALLORA nascondilo (return False).
            # Nota: cerchiamo " 200 " con gli spazi per non confonderlo 
            # con un pezzo di data o IP.
            if ' 200 ' in msg:
                return False
            
            # Se è /health ma il codice è 404, 500, 503... MOSTRALO!
            return True
            
        # Per tutte le altre rotte, mostra sempre.
        return True

# Recuperiamo il logger di Werkzeug (il server di Flask)
werkzeug_logger = logging.getLogger('werkzeug')

# Aggiungiamo il nostro filtro
werkzeug_logger.addFilter(HealthCheckFilter())

logger = logging.getLogger(__name__)

# Upload private key for JWT signature
key_path = os.getenv('JWT_PRIVATEKEY_SECRET_PATH', '')

try:
    with open(key_path, 'rb') as f:
        PRIVATE_KEY = f.read()
except FileNotFoundError:
    logger.error(f"ERRORE: Impossibile trovare la chiave privata")
    PRIVATE_KEY = None

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
import user_service_pb2
import user_service_pb2_grpc

app = Flask(__name__)

# --- Database setup ---
db_user = os.getenv('USER_DB')
db_password = os.getenv('PASSWORD_DB')
db_host = os.getenv('HOST_DB')
db_port = os.getenv('USER_DB_PORT')
db_name = os.getenv('NAME_DB')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    try:
        db.create_all()
    except Exception:
        pass

# Redis per idempotenza delle API
redis_client = redis.Redis(
    host=os.getenv('USER_REDIS_HOST', 'user-cache.default.svc.cluster.local'),
    port=int(os.getenv('USER_REDIS_HOST_PORT', 6379)),
    db=0,
    decode_responses=True,
)

black_list = redis.Redis(
    host=os.getenv('USER_REDIS_HOST', 'user-cache.default.svc.cluster.local'),
    port=int(os.getenv('USER_REDIS_HOST_PORT', 6379)),
    db=1,
    decode_responses=True,
)

# --- gRPC client setup for user validation ---
service_config = """{
    "loadBalancingConfig": [{"round_robin": {}}],
    "methodConfig": [{
        "name": [{"service": "DeleteUserInterestsService"}],
        "retryPolicy": {
            "maxAttempts": 3,
            "initialBackoff": "0.5s",
            "maxBackoff": "3s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED"]
        },
        "timeout": "5s"
    }]
}"""

options=[
    ('grpc.service_config', service_config),
    ('grpc.keepalive_time_ms', 10000),  # Ping ogni 10s
    ('grpc.keepalive_timeout_ms', 5000),
]

gRPC_HOST = os.getenv('gRPC_HOST', 'data-collector-headless.default.svc.cluster.local')
gRPC_HOST_PORT = os.getenv('gRPC_HOST_PORT', '50051')
target = f'dns:///{gRPC_HOST}:{gRPC_HOST_PORT}'

channel = grpc.insecure_channel(target, options=options)
stub = user_service_pb2_grpc.DeleteUserInterestsServiceStub(channel)

# --- Prometheus Metrics Variables ---
REGISTER_REQUEST_COUNT = Counter('request_add_user', 'Richieste Aggiunta Utente', ['endpoint'])
LOGIN_REQUEST_COUNT = Counter('request_login_user', 'Richieste Login Utente', ['endpoint'])

@app.route('/health')
def health_check():
    return jsonify({"status": "ok"}), 200

@app.route('/auth/login', methods=['POST'])
def login_user():
    """Effettua il login di un utente."""

    LOGIN_REQUEST_COUNT.labels(endpoint='/auth/login').inc()

    request_id = request.headers.get('X-Request-ID')
    
    if not request_id:
        return jsonify({"error": "X-REQUEST-ID missing in header"}), 400

    if not PRIVATE_KEY:
        logger.error("Private key not available.")
        return jsonify({"error": "Server error"}), 500

    cache_key = f"login:{request_id}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']
    
    data = request.get_json() or {}
    if not data or 'email' not in data or 'password' not in data:
        return jsonify({"error": "Missing email or password"}), 400

    email = data['email']
    password = data['password']

    user = User.login(email, password)
    if user:
        now_utc = datetime.now(timezone.utc)
        
        # Creazione e firma del JWT
        payload = {
            'sub': email,
            'client_id': str(uuid.uuid4()),
            'iat': now_utc,
            'exp': now_utc + timedelta(minutes=10)
        }
        token = jwt.encode(payload, PRIVATE_KEY, algorithm="RS256")

        response_body = {"access_token": token, "token_type": "Bearer", "expires_in": "10m"}
        status_code = 200
    else:
        response_body = {"error": "Invalid credentials"}
        status_code = 401

    cache_packet = {"body": response_body, "status_code": status_code}
    redis_client.setex(cache_key, 180, json.dumps(cache_packet))
    return jsonify(response_body), status_code

@app.route('/auth/register', methods=['POST'])
def register_user():

    REGISTER_REQUEST_COUNT.labels(endpoint='/auth/register').inc()

    """Registra un nuovo utente. Applica idempotenza tramite Redis."""
    request_id = request.headers.get('X-Request-ID')
    if not request_id:
        return jsonify({"error": "X-REQUEST-ID missing in header"}), 400

    cache_key = f"register:{request_id}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        logger.info(f"Cache hit for register: {cache_key}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    data = request.get_json() or {}
    if not data or 'email' not in data or 'password' not in data:
        return jsonify({"errore": "Email or password missing"}), 400

    email = data['email']
    password = data['password']
    nome = data.get('nome')
    cognome = data.get('cognome')
    logger.info(f"Attempting registration for {email} -> {nome} {cognome}")

    success = User.add_user(email, password, nome, cognome)
    if success:
        response_body = {"message": "Utente registrato con successo", "email_request": email, "status": True}
        status_code = 201
    else:
        response_body = {"message": "Utente già registrato, email presente in archivio.", "email_request": email, "status": False}
        status_code = 409

    cache_packet = {"body": response_body, "status_code": status_code}
    redis_client.setex(cache_key, 3600, json.dumps(cache_packet))
    return jsonify(response_body), status_code


@app.route('/users/delete', methods=['POST'])
def delete_user():
    """Elimina un utente. Usa cache con TTL breve per AT-MOST-ONCE."""
    email = request.headers.get('X-User-Email')
    request_id = request.headers.get('X-Request-ID')
    client_id = request.headers.get('X-Client-ID')

    if not request_id or not client_id or not email:
        return jsonify({"error": "X-REQUEST-ID/X-Client-ID/X-User-Email mancante nell'header della richiesta HTTP."}), 400

    cache_key = f"{client_id}:delete:{request_id}"
    cached_data = redis_client.get(cache_key)

    if cached_data:
        logger.info(f"Cache hit for delete: {cache_key}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    success = User.delete_user(email)

    if success:
        response_body = {"message": "utente correttamente eliminato dall'archivio", "email_request": email, "status": True}
        status_code = 200

        # Chiamata gRPC per eliminare le preferenze associate all'utente
        try:
            response = stub.DeleteUserInterests(user_service_pb2.DeleteUserInterestsRequest(email=email), timeout=5)

            if response.status == 0:
                response_body['details'] = "interessi utente eliminati con successo"
            else:
                logger.error(f"Errore eliminazione interessi utente via gRPC: status {response.status}")
        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e.code().name} - {e.details()}")
    else:
        response_body = {"message": "utente non presente in archivio", "email_request": email, "status": False}
        status_code = 404

    cache_packet = {"body": response_body, "status_code": status_code}
    redis_client.setex(cache_key, 180, json.dumps(cache_packet))

    black_list.set(f"blacklist:{client_id}", "true", ex=900)

    return jsonify(response_body), status_code


if __name__ == '__main__':

    start_http_server(8001)
    logger.info("REST Server listening on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False)          
