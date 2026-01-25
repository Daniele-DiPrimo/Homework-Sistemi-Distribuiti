"""
Questo modulo espone endpoint REST per registrare gli aeroporti di interesse
e consultare informazioni sui voli. Si integra con:
- un DB MySQL (via SQLAlchemy)
- Redis per caching e idempotenza
- un servizio gRPC per validare l'esistenza dell'utente
- scheduler per aggiornamenti periodici
"""

import os
from flask import Flask, request, jsonify, g, current_app
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func, insert
import requests
import extensions
from models import AirportsOfInterest, Flights
import tasks
import grpc
import sys
from datetime import datetime, timedelta
import redis
import json
from circuit_breaker import CircuitBreakerOpenException
import logging
from kafkaClient import KafkaProducer
from concurrent import futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from prometheus_client import start_http_server, Counter, Gauge

# --- Health Check Logging Filter ---
class HealthCheckFilter(logging.Filter):
    def filter(self, record):
        msg = record.getMessage()

        if '/health' in msg:
            if ' 200 ' in msg:
                return False
            
            return True
            
        return True

werkzeug_logger = logging.getLogger('werkzeug')

werkzeug_logger.addFilter(HealthCheckFilter())

logger = logging.getLogger(__name__)

# --- gRPC Imports ---
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
import user_service_pb2, user_service_pb2_grpc

# --- Flask App Setup ---
app = Flask(__name__)

# --- Database setup (SQLAlchemy) ---
db_user = os.getenv('FLIGHTSDB_USER')
db_password = os.getenv('FLIGHTSDB_PASSWORD')
db_host = os.getenv('FLIGHTSDB_HOST')
db_port = os.getenv('FLIGHTSDB_PORT')
db_name = os.getenv('FLIGHTSDB_DATABASE')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
extensions.db.init_app(app)

# --- Create tables if not exist ---
with app.app_context():
    extensions.db.create_all()

# --- Redis caches ---
requests_cache = redis.Redis(
    host=os.getenv('DATA_REDIS_HOST', 'data-cache.default.svc.cluster.local'),
    port=int(os.getenv('DATA_REDIS_HOST_PORT', 6379)),
    db=0,
    decode_responses=True,
)

# --- Scheduler setup (periodic tasks) ---
extensions.scheduler.init_app(app)
extensions.scheduler.start()

# --- metrics variables ---
REQUEST_COUNT = Counter(
    'request_add_airport_total',
    'Richieste alla funzione add-airport-of-interest', 
    ['endpoint']
)
ERROR_COUNT = Counter(
    'error_add_airport_total',    
    'Richieste fallite alla funzione add-airport-of-interest', 
    ['endpoint']
)
ERROR_COUNT_OPENSKY = Counter(
    'error_opensky_total',    
    'Richieste fallite alla funzione add-airport-of-interest', 
    ['endpoint']
)
LATEST_RESPONSE_TIME = Gauge(
    'airport_add_response_time_seconds', 
    'Tempo di risposta ultima chiamata add_airport', 
    ['endpoint']
)
HTTP_REQUESTS_TOTAL = Counter(
    'http_requests_total', 
    'Totale richieste HTTP gestite', 
    ['method', 'endpoint', 'status_code']
)
LATEST_RESPONSE_TIME_BACKGROUNT_TASK = Gauge(
    'background_task_response_time_seconds', 
    'Tempo di risposta ultima chiamata background task'
)

# --- gRPC Service for deleting user interests ---
class DeleteUserInterestsHandler(user_service_pb2_grpc.DeleteUserInterestsServiceServicer):
    def DeleteUserInterests(self, request, context):
        email = request.email

        with app.app_context():
            try:
                stmt = extensions.db.delete(AirportsOfInterest).where(AirportsOfInterest.email == email)
                extensions.db.session.execute(stmt)
                extensions.db.session.commit()
                logger.info(f"Deleted interests for user {email}")
            except SQLAlchemyError as e:
                logger.error(f"Database error during deletion of interests for {email}: {str(e)}")
                return user_service_pb2.DeleteUserInterestsResponse(status=1, message="Database error")

            logging.info(f"Deleted interests for user {email}")
            return user_service_pb2.DeleteUserInterestsResponse(status=0, message="User interests deleted successfully")

# --- gRPC Server Setup ---
def run_grpc_server():
    server_options = [
        ('grpc.keepalive_time_ms', 10000),          # Ping ogni 10s se inattivo
        ('grpc.keepalive_timeout_ms', 5000),       # Timeout risposta ping 5s
        ('grpc.keepalive_permit_without_calls', 1), # Pinga anche senza richieste in corso
        ('grpc.http2.max_pings_without_data', 0),   # Permetti ping illimitati
    ]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=server_options)
    user_service_pb2_grpc.add_DeleteUserInterestsServiceServicer_to_server(DeleteUserInterestsHandler(), server)

    gRPC_HOST_PORT = os.getenv('gRPC_PORT', '50051')
    server.add_insecure_port('[::]:' + gRPC_HOST_PORT)
    logger.info("gRPC Server listening on port " + gRPC_HOST_PORT)
    
    server.start()
    server.wait_for_termination()

# --- Kafka producer wrapper (confluent-kafka) ---
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')

if not bootstrap_servers:
    logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable not set.")

producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'acks': 'all',
    'batch.size': 10000,
    'max.in.flight.requests.per.connection': 1,
    'retries': 3,
    'linger.ms': 100,
}

extensions.kafka_producer = KafkaProducer(producer_config)

# --- Helper Background Task ---
def background_fetch_and_notify(app, interests, user_email):
    """
    Esegue il fetch parallelo dei dati OpenSky, salva su DB e notifica Kafka.
    Viene eseguito in un thread separato.
    """
    with app.app_context():
        with LATEST_RESPONSE_TIME_BACKGROUNT_TASK.time():
            logger.info(f"Starting background task for user {user_email} with {len(interests)} airports.")
            fetched_results = []
            airports_without_flights = []
            # Parallel Fetch Data
            with ThreadPoolExecutor(max_workers=5) as executor:

                future_to_interest = {
                    executor.submit(tasks.fetch_data, interest.get('icao')): interest 
                    for interest in interests
                }

                for future in as_completed(future_to_interest):
                    interest = future_to_interest[future]
                    icao = interest.get('icao')

                    try:
                        flights_data = future.result()
                    
                        interest['flights_count'] = len(flights_data)
                        if flights_data:
                            fetched_results.extend(flights_data)
                        else:
                            airports_without_flights.append(icao)

                    except CircuitBreakerOpenException:
                        ERROR_COUNT_OPENSKY.labels(endpoint='/airport-of-interest/add').inc()
                        logger.error(f"Circuit Breaker OPEN for {icao}. Skipping fetch.")
                        interest['flights_count'] = 0
                    
                    except FileNotFoundError:
                        logger.error(f"SECRETS MISSING processing {icao}. Cannot fetch token.")
                        interest['flights_count'] = 0
                    
                    except Exception as e:
                        logger.error(f"Generic error processing {icao}: {e}")
                        interest['flights_count'] = 0

            if airports_without_flights:
                logger.info(f"No flights found for airports: {', '.join(airports_without_flights)}")

            if fetched_results:
                try:
                    stmt = insert(Flights).values(fetched_results)
                    stmt = stmt.prefix_with('IGNORE') 
                    extensions.db.session.execute(stmt)
                    extensions.db.session.commit()
                    logger.info(f"Saved {len(fetched_results)} flights to DB.")
                except SQLAlchemyError as e:
                    extensions.db.session.rollback()
                    logger.error(f"DB Error during background insert: {e}")

            try:
                tasks.send_to_kafka(interests, user_email)
                logger.info("Notification sent to Kafka.")
            except Exception as e:
                logger.error(f"Kafka Error: {e}")

@app.route('/health')
def health_check():
    return jsonify({"status": "ok"}), 200

# --- Middleware HTTP requests counter ---
@app.after_request
def monitor_requests(response):
   
    if request.path == '/metrics' or request.path == '/health_check':
        return response

    endpoint_name = request.endpoint if request.endpoint else 'unknown'

    HTTP_REQUESTS_TOTAL.labels(
        method=request.method,
        endpoint=endpoint_name,
        status_code=response.status_code
    ).inc()

    return response


@app.before_request
def headers_check():
    if request.path in ['/health', '/metrics']:
        return None 

    g.client_id = request.headers.get('X-Client-ID')
    g.request_id = request.headers.get('X-Request-ID')
    g.email = request.headers.get('X-User-Email')

    if not g.client_id:
        return jsonify({"error": "Header 'X-Client-ID' missing"}), 400
    if not g.request_id:
        return jsonify({"error": "Header 'X-Request-ID' missing"}), 400
    if not g.email:
        return jsonify({"error": "Header 'X-User-Email' missing"}), 400

# --- Routes ---
@app.route('/airport-of-interest/add', methods=['POST'])
def add_airports_of_interest():
    """Aggiunge aeroporti di interesse.
            Flusso ottimizzato:
            1. Check Cache
            2. Salva preferenze su DB (Veloce)
            3. Lancia thread background per fetch dati (Async)
            4. Ritorna subito 202 Accepted
    """

    with LATEST_RESPONSE_TIME.labels(endpoint='/airport-of-interest/add').time():
    
        REQUEST_COUNT.labels(endpoint='/airport-of-interest/add').inc()
        
        cache_key = f"{g.client_id}:airport_add:{g.request_id}"
        cached_data = requests_cache.get(cache_key)
        
        if cached_data:
            response_json = json.loads(cached_data)
            return jsonify(response_json['body']), response_json['status_code']
    
        data = request.get_json() or {}
        interests = data.get('airports')

        if not interests:
            return jsonify({"error": "No airports specified"}), 400

        try:
            new_entries = []
            for interest in interests:
                new_entries.append(AirportsOfInterest(
                    email=g.email,
                    icao=interest.get('icao'),
                    high_value=interest.get('high_value'),
                    low_value=interest.get('low_value')
                ))
        
            extensions.db.session.add_all(new_entries)
            extensions.db.session.commit()

        except IntegrityError:
            extensions.db.session.rollback()
            response_body = {
                "error": "Duplicate entry",
                "details": "One or more airports are already present."
            }
            ERROR_COUNT.labels(endpoint='/airport-of-interest/add').inc()

            cache_packet = {"body": response_body, "status_code": 409}
            requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
            return jsonify(response_body), 409

        except SQLAlchemyError as e:
            extensions.db.session.rollback()
            ERROR_COUNT.labels(endpoint='/airport-of-interest/add').inc()
            return jsonify({"error": "Database error", "details": str(e)}), 500

        # Asynchronous Background Task
        app = current_app._get_current_object()
    
        interests_copy = json.loads(json.dumps(interests)) 
        user_email = g.email

        thread = threading.Thread(
            target=background_fetch_and_notify,
            args=(app, interests_copy, user_email)
        )
        thread.start()

        response_body = {
            "message": "Airports added successfully",
            "details": "Data collection started in background"
        }
    
        cache_packet = {"body": response_body, "status_code": 202}
        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
    
        return jsonify(response_body), 202


@app.route('/get-flights/latest', methods=['GET'])
def get_latest_flights():
    cache_key = f"{g.client_id}:flights_latest:{g.request_id}"
    cached_data = requests_cache.get(cache_key)
    if cached_data:
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    airport = request.args.get('airport')
    if not airport:
        return jsonify({"message": "Parameter 'airport' missing"}), 400

    try:
        stmt = extensions.db.select(Flights).where(Flights.estDepartureAirport == airport).order_by(Flights.firstSeen.desc()).limit(1)
        last_departure = extensions.db.session.execute(stmt).scalars().first()

        stmt = extensions.db.select(Flights).where(Flights.estArrivalAirport == airport).order_by(Flights.lastSeen.desc()).limit(1)
        last_arrival = extensions.db.session.execute(stmt).scalars().first()

        response_body = {
            "last_departure": last_departure.to_dict() if last_departure is not None else "No departures for this airport",
            "last_arrival": last_arrival.to_dict() if last_arrival is not None else "No arrivals for this airport",
        }

        cache_packet = {"body": response_body, "status_code": 200}
        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 200

    except requests.exceptions.RequestException:
        return jsonify({"error": "Error during api call"}), 500


@app.route('/airport-of-interest/average', methods=['GET'])
def average():
    cache_key = f"{g.client_id}:flight_average:{g.request_id}"
    cached_data = requests_cache.get(cache_key)
    if cached_data:
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    airport = request.args.get('airport')
    numberOfDays = request.args.get('numberOfDays', default=1, type=int)

    if not airport or not numberOfDays:
        return jsonify({"errore": "Dati mancanti. Inserisci l'aeroporto e il numero di giorni"}), 400

    try:
        limit_date = (datetime.now() - timedelta(days=numberOfDays)).replace(hour=0, minute=0, second=0, microsecond=0)

        departures_count = extensions.db.session.query(func.count(Flights.id)).filter(
            Flights.estDepartureAirport == airport,
            Flights.firstSeen >= limit_date
        ).scalar()

        arrivals_count = extensions.db.session.query(func.count(Flights.id)).filter(
            Flights.estArrivalAirport == airport,
            Flights.lastSeen >= limit_date
        ).scalar()

        
        avg_departures = departures_count / numberOfDays if departures_count else 0
        avg_arrivals = arrivals_count / numberOfDays if arrivals_count else 0

        response_body = {
            "aeroporto_selezionato": airport,
            "numero_di_giorni_analizzati": numberOfDays,
            "numero_partenze": departures_count,
            "numero_di_arrivi": arrivals_count,
            "media_giornaliera_voli_in_partenza": round(avg_departures, 2),
            "media_giornaliera_voli_in_arrivo": round(avg_arrivals, 2),
        }

        cache_packet = {"body": response_body, "status_code": 200}
        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 200

    except Exception as e:
        return jsonify({"error": "Error in api", "details": str(e)}), 500


if __name__ == '__main__':
    
    start_http_server(8000) 

    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()

    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)