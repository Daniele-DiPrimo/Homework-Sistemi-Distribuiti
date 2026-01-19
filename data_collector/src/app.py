"""
Data Collector API

Questo modulo espone endpoint REST per registrare gli aeroporti di interesse
e consultare informazioni sui voli. Si integra con:
- un DB MySQL (via SQLAlchemy)
- Redis per caching e idempotenza
- un servizio gRPC per validare l'esistenza dell'utente
- scheduler per aggiornamenti periodici
"""

import os
from flask import Flask, request, jsonify, g
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
import threading

logger = logging.getLogger(__name__)

# Ensures we can import generated gRPC stubs in runtime
sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
import user_service_pb2, user_service_pb2_grpc

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

with app.app_context():
    # Crea le tabelle se non esistono
    extensions.db.create_all()

# --- Redis caches ---
requests_cache = redis.Redis(
    host=os.getenv('REDIS_HOST', 'data-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=0,
    decode_responses=True,
)

# --- Scheduler setup (periodic tasks) ---
extensions.scheduler.init_app(app)
extensions.scheduler.start()

class DeleteUserInterestsHandler(user_service_pb2_grpc.DeleteUserInterestsServiceServicer):
    def DeleteUserInterests(self, request, context):
        email = request.email

        with app.app_context():
            try:
                stmt = extensions.db.delete(AirportsOfInterest).where(AirportsOfInterest.email == email)
                extensions.db.session.execute(stmt)
                extensions.db.session.commit()
            except SQLAlchemyError as e:
                logger.error(f"Database error during deletion of interests for {email}: {str(e)}")
                return user_service_pb2.DeleteUserInterestsResponse(status=1, message="Database error")

            return user_service_pb2.DeleteUserInterestsResponse(status=0, message="User interests deleted successfully")

def run_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_service_pb2_grpc.add_DeleteUserInterestsServiceServicer_to_server(DeleteUserInterestsHandler(), server)
    server.add_insecure_port('[::]:50051')
    logger.info("gRPC Server listening on port 50051")
    server.start()
    server.wait_for_termination()

# --- Kafka producer wrapper (confluent-kafka) ---
producer_config = {
    'bootstrap.servers': 'broker-kafka-1:9092,broker-kafka-2:9092,broker-kafka-3:9092',
    'acks': 'all',
    'batch.size': 10000,
    'max.in.flight.requests.per.connection': 1,
    'retries': 3,
    'linger.ms': 100,
}

extensions.kafka_producer = KafkaProducer(producer_config)

@app.before_request
def headers_check():
    g.client_id = request.headers.get('X-Client-ID')
    g.request_id = request.headers.get('X-Request-ID')
    g.email = request.headers.get('X-User-Email')

    # controllo presenza header richiesti
    if not g.client_id:
        return jsonify({"error": "Header 'X-Client-ID' missing"}), 400
    if not g.request_id:
        return jsonify({"error": "Header 'X-Request-ID' missing"}), 400
    if not g.email:
        return jsonify({"error": "Header 'X-User-Email' missing"}), 400

# --- Routes ---
@app.route('/airport-of-interest/add', methods=['POST'])
def add_airports_of_interest():
    """Aggiunge aeroporti di interesse per l'utente autenticato.

    Flusso:
    - utilizza la cache per idempotenza
    - salva le preferenze su DB
    - avvia fetch dei voli e inserimento in DB
    - invia le statistiche a Kafka
    """
    cache_key = f"{g.client_id}:airport_add:{g.request_id}"
    cached_data = requests_cache.get(cache_key)
    if cached_data:
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    data = request.get_json() or {}
    interests = data.get('airports')

    # Se nessun aeroporto specificato, ritorno errore
    if not interests:
        return jsonify({"error": "No airports specified"}), 400

    try:
        for interest in interests:
            extensions.db.session.add(AirportsOfInterest(
                email=g.email,
                icao=interest.get('icao'),
                high_value=interest.get('high_value'),
                low_value=interest.get('low_value')
            ))
        extensions.db.session.commit()

    except IntegrityError:
        extensions.db.session.rollback()
        response_body = {"error": "Duplicate entry or constraint violation",
                         "details": "One or more airports are already present for this user."}
        cache_packet = {"body": response_body, "status_code": 409}
        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 409

    except SQLAlchemyError as e:
        extensions.db.session.rollback()
        return jsonify({"error": "Database error, cant add the airports", "details": str(e)}), 500

    response_body = {"message": "Airports of interest added"}
    # Dopo salvataggio, recupero dati voli e invio a Kafka
    try:
        airports_without_flights = []

        for interest in interests:
            result = tasks.fetch_data(interest.get('icao'))
            interest['flights_count'] = len(result)

            if not result:
                airports_without_flights.append(interest.get('icao'))
                continue

            stmt = insert(Flights).values(result)
            stmt = stmt.prefix_with('IGNORE')
            extensions.db.session.execute(stmt)

        extensions.db.session.commit()

        tasks.send_to_kafka(interests, g.email)

        response_body['flights_not_found_for'] = airports_without_flights
        cache_packet = {"body": response_body, "status_code": 201}
        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 201

    except CircuitBreakerOpenException:
        response_body["warning"] = "Connection to OpenSky failed. Circuit open."
        return jsonify(response_body), 201
    except FileNotFoundError:
        response_body["warning"] = "Secrets not found for token retrieval."
        return jsonify(response_body), 201
    except Exception as e:
        logger.error(str(e))
        response_body["warning"] = f"Generic error: {e}"
        return jsonify(response_body), 201


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
        # limit_date: data di inizio per la ricerca (mezzanotte del giorno calcolato)
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
    # Avvio del server gRPC in thread separato per non bloccare Flask
    grpc_thread = threading.Thread(target=run_grpc_server, daemon=True)
    grpc_thread.start()

    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)