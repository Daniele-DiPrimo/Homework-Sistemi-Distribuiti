import os
from flask import Flask, request, jsonify, g
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func
import requests
from extensions import db, scheduler
from models import AirportsOfInterest, Flights
import tasks
import grpc
import sys
from datetime import datetime, timedelta
import redis
import json
from sqlalchemy import func

sys.path.append(os.path.join(os.path.dirname(__file__), "grpc_generated"))
import user_service_pb2, user_service_pb2_grpc

app = Flask(__name__)

#setup database
db_user = os.getenv('FLIGHTSDB_USER')
db_password = os.getenv('FLIGHTSDB_PASSWORD')
db_host = os.getenv('FLIGHTSDB_HOST')
db_port = os.getenv('FLIGHTSDB_PORT')
db_name = os.getenv('FLIGHTSDB_DATABASE')

SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
app.config["SQLALCHEMY_DATABASE_URI"] = SQLALCHEMY_DATABASE_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

with app.app_context():
    db.create_all()

email_check_cache = redis.Redis(
    host=os.getenv('REDIS_HOST', 'data-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db = 0,
    decode_responses = True
)

requests_cache = redis.Redis(
    host=os.getenv('REDIS_HOST', 'data-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db = 1,
    decode_responses = True
)

#setup scheduler
scheduler.init_app(app)
scheduler.start()


#setup grpc channel
service_config = """{
    "methodConfig": [{
        "name": [{"service": "CheckUserService"}],
        "retryPolicy": {
            "maxAttempts": 3,
            "initialBackoff": "0.5s",
            "maxBackoff": "3s",
            "backoffMultiplier": 2,
            "retryableStatusCodes": ["UNAVAILABLE"]
        },
        "timeout": "5s"
    }]
}"""

options = [('grpc.service_config', service_config)]
channel = grpc.insecure_channel('user-manager:50051', options=options)
stub = user_service_pb2_grpc.CheckUserServiceStub(channel)


#middleware
@app.before_request
def email_check():
    g.client_id = request.headers.get('X-Client-ID')
    g.request_id = request.headers.get('X-Request-ID')
    g.email = request.headers.get('X-User-Email')

    if not g.client_id:
        return jsonify({"error": "Header 'X-Client-ID' missing"}), 400
    
    if not g.request_id:
        return jsonify({"error": "Header 'X-Request-ID' missing"}), 400

    if not g.email:
        return jsonify({"error": "Header 'X-User-Email' missing"}), 400
    
    cache_key = f"{g.client_id}:{g.request_id}"
    cached_data = email_check_cache.get(cache_key)

    if cached_data: 
        response_json = json.loads(cached_data)

        if response_json["status_code"] == 200:
            return None
            
        return jsonify(response_json['body']), response_json['status_code']


    # CHECK SULLA CACHE, 


    try:
        response = stub.CheckUserExists(
            user_service_pb2.UserCheckRequest(email = g.email),
            timeout=5
        )

        response_json = {
            "status": response.status,
            "message": response.message
        }

        cache_packet = { 
            "body": response_json,
            "status_code": 200
        }

        if response.status == 0:
            email_check_cache.setex(cache_key, 300, json.dumps(cache_packet))
            return None
        else:
            cache_packet["status_code"] = 401
            email_check_cache.setex(cache_key, 300, json.dumps(cache_packet))
            return jsonify({
                "error": "User not exists", 
                "details": response.message
            }), 401
                        
    except grpc.RpcError as e:
        print(f"ERROR: {e.code().name} - {e.details()}")

        return jsonify({
            "error": "Validation service not available",
        }), 503


#routes
@app.route('/airport-of-interest/add', methods=['POST'])
def add_airports_of_interest():

    cache_key = f"{g.client_id}:airport_add:{g.request_id}"
    cached_data = requests_cache.get(cache_key)

    if cached_data: 
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']
    
    data = request.get_json()
    airports = tuple(data.get('airports'))


    #logica di business
    try:
        for airport in airports:
            db.session.add(AirportsOfInterest(email=g.email, icao=airport))

        db.session.commit()

        tasks.fetch_and_update_db(airports)

        response_body = {"message": "Airports added"}
        cache_packet = { 
            "body": response_body,
            "status_code": 201
        }

        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))

        return jsonify(response_body), 201
    
    except IntegrityError as e:
        db.session.rollback()

        response_body = {
            "error": "Duplicate entry or constraint violation",
            "details": "One or more airports are already present for this user."
        }

        cache_packet = { 
            "body": response_body,
            "status_code": 409
        }

        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 409

    except SQLAlchemyError as e:
        db.session.rollback()
        
        return jsonify({
            "error": "Database error", 
            "details": str(e)
        }), 500

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
        stmt = db.select(Flights)\
        .where(Flights.estDepartureAirport == airport)\
        .order_by(Flights.firstSeen.desc())\
        .limit(1)
        last_departure = db.session.execute(stmt).scalars().first()

        stmt = db.select(Flights)\
        .where(Flights.estArrivalAirport == airport)\
        .order_by(Flights.lastSeen.desc())\
        .limit(1)
        last_arrival = db.session.execute(stmt).scalars().first()

        response_body = {
            "last_departure": last_departure.to_dict(),
            "last_arrival": last_arrival.to_dict()
        }

        cache_packet = { 
            "body": response_body,
            "status_code": 200
        }

        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 200
    
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "Error during api call"
        }), 500

@app.route('/airport-of-interest/average', methods=['GET'])
def average():

    cache_key = f"{g.client_id}:flight_average:{g.request_id}"
    cached_data = requests_cache.get(cache_key)

    if cached_data: 
        response_json = json.loads(cached_data)
        return jsonify(response_json['body']), response_json['status_code']

    airport = request.args.get('airport')
    numberOfDays = request.args.get('numberOfDays')

    if not airport or not numberOfDays:
        return jsonify({"errore" : " Dati Mancanti. Inserisci l'aeroporto e il numero di giorni"})

    try: 
        #limit_date è la data dopo il quale dobbiamo cercare i voli. E' uguale alla data di oggi - i giorni scelti dall'utente.
        # il .raplace ci consente di partire dalla mezzanotte del giorno limit_date. Senza questo il limit_date aveva l'orario del giorno datetime.now()
        limit_date = (datetime.now() - timedelta(days=numberOfDays)).replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"limit_date {limit_date}")

        #func è una libreria di sqlAlchemy che ha la funzione count --> la query ritorna il numero di voli 
        # in departures_count ci sarà il numero di flight.id filtrati per data e estArrivalAirport
        departures_count = db.session.query(func.count(Flights.id)).filter(
            Flights.estDepartureAirport == airport,
            Flights.firstSeen >= limit_date
        ).scalar()

        arrivals_count = db.session.query(func.count(Flights.id)).filter(
            Flights.estArrivalAirport == airport,
            Flights.lastSeen >= limit_date
        ).scalar()

        average_departures = departures_count / numberOfDays
        average_arrivals = arrivals_count / numberOfDays

        response_body = {
            "aeroporto_selezionato": airport,
            "numero_di_giorni_analizzati": numberOfDays,
            "numero_partenze": departures_count,
            "numero_di_arrivi": arrivals_count,
            "media_giornaliera_voli_in_partenza": round(average_departures, 2),
            "media_giornaliera_voli_in_arrivo": round(average_arrivals, 2)
        }

        cache_packet = { 
            "body": response_body,
            "status_code": 200
        }

        requests_cache.setex(cache_key, 300, json.dumps(cache_packet))
        return jsonify(response_body), 200

    except Exception as e:
        return jsonify({
            "error": "Error in api",
            "details": str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)