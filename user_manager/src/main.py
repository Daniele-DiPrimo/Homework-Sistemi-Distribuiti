"""User Manager microservice

Espone API REST per registrare/eliminare utenti e un server gRPC per
verificare l'esistenza di un utente (usato da altri servizi).
"""

from flask import Flask, request, jsonify
import grpc
from concurrent import futures
import sys
import os
import threading
import json
import redis
import logging
from extensions import db
from user import User

logger = logging.getLogger(__name__)

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
    host=os.getenv('REDIS_HOST', 'user-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)


class CheckUserHandler(user_service_pb2_grpc.CheckUserServiceServicer):
    def CheckUserExists(self, request, context):
        """gRPC handler: verifica l'esistenza di un'email nel DB."""
        email = request.email
        logger.info(f"Checking existence of {email} in DB")
        with app.app_context():
            exists = User.user_exist(email)

        if exists:
            return user_service_pb2.UserCheckResponse(status=0, message="UTENTE TROVATO")
        else:
            return user_service_pb2.UserCheckResponse(status=1, message="UTENTE NON TROVATO")


def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_CheckUserServiceServicer_to_server(CheckUserHandler(), server)
    server.add_insecure_port('[::]:50051')
    logger.info("gRPC Server listening on port 50051")
    server.start()
    server.wait_for_termination()


@app.route('/register', methods=['POST'])
def register_user():
    """Registra un nuovo utente. Applica idempotenza tramite Redis."""
    request_id = request.headers.get('X-Request-ID')
    client_id = request.headers.get('X-Client-ID')
    if not request_id or not client_id:
        return jsonify({"error": "X-REQUEST-ID/X-ClientID mancante nell'header della richiesta HTTP."}), 400

    cache_key = f"{client_id}:register:{request_id}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        logger.info(f"Cache hit for register: {cache_key}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    data = request.get_json() or {}
    if not data or 'email' not in data:
        return jsonify({"errore": "EMAIL NON INSERITA"}), 400

    email = data['email']
    nome = data.get('nome')
    cognome = data.get('cognome')
    logger.info(f"Attempting registration for {email} -> {nome} {cognome}")

    success = User.add_user(email, nome, cognome)
    if success:
        response_body = {"message": "Utente registrato con successo", "email_request": email, "status": True}
        status_code = 201
    else:
        response_body = {"message": "Utente già registrato, email presente in archivio.", "email_request": email, "status": False}
        status_code = 409

    cache_packet = {"body": response_body, "status_code": status_code}
    redis_client.setex(cache_key, 3600, json.dumps(cache_packet))
    return jsonify(response_body), status_code


@app.route('/delete', methods=['POST'])
def delete_user():
    """Elimina un utente. Usa cache con TTL breve per AT-MOST-ONCE."""
    request_id = request.headers.get('X-Request-ID')
    client_id = request.headers.get('X-Client-ID')
    if not request_id or not client_id:
        return jsonify({"error": "X-REQUEST-ID/X-Client-ID mancante nell'header della richiesta HTTP."}), 400

    cache_key = f"{client_id}:delete:{request_id}"
    cached_data = redis_client.get(cache_key)
    if cached_data:
        logger.info(f"Cache hit for delete: {cache_key}")
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    data = request.get_json() or {}
    if 'email' not in data:
        return jsonify({"errore": "email non inserita. Perfavore inserisci email"}), 400

    email = data['email']
    success = User.delete_user(email)
    if success:
        response_body = {"message": "utente correttamente eliminato dall'archivio", "email_request": email, "status": True}
        status_code = 200
    else:
        response_body = {"message": "utente non presente in archivio", "email_request": email, "status": False}
        status_code = 404

    cache_packet = {"body": response_body, "status_code": status_code}
    redis_client.setex(cache_key, 180, json.dumps(cache_packet))
    return jsonify(response_body), status_code


if __name__ == '__main__':
    # Avvio del server gRPC in thread separato per non bloccare Flask
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()

    logger.info("REST Server listening on port 5000")
    app.run(host='0.0.0.0', port=5000, debug=False)          
