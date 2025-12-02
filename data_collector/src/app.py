import os
from flask import Flask, request, jsonify
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy import func
import requests
from extensions import db, scheduler
from models import AirportsOfInterest, Flights
import tasks
import grpc
import sys
from datetime import datetime, timedelta
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
    email = request.headers.get('X-User-Email')

    if not email:
        return jsonify({"error": "Header 'X-User-Email' missing"}), 400


    # CHECK SULLA CACHE, 


    try:
        response = stub.CheckUserExists(
            user_service_pb2.UserCheckRequest(email = email),
            timeout=5
        )

        if response.status == 1:
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
    email = request.headers.get('X-User-Email')
    data = request.get_json()
    airports = tuple(data.get('airports'))


    #logica di business
    try:
        for airport in airports:
            db.session.add(AirportsOfInterest(email=email, icao=airport))

        db.session.commit()

        tasks.fetch_and_update_db(airports)
        return jsonify({"message": "Airports added"}), 201
    
    except IntegrityError as e:
        db.session.rollback()
        
        return jsonify({
            "error": "Duplicate entry or constraint violation",
            "details": "One or more airports are already present for this user."
        }), 409

    except SQLAlchemyError as e:
        db.session.rollback()
        
        return jsonify({
            "error": "Database error", 
            "details": str(e)
        }), 500

@app.route('/get-flights/latest', methods=['GET'])  
def get_latest_flight():
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

        return jsonify({
            "last_departure": last_departure.to_dict(),
            "last_arrival": last_arrival.to_dict()
        }), 200
    except requests.exceptions.RequestException as e:
        return jsonify({
            "error": "Error during api call"
        }), 500

@app.route('/airport-of-interest/average', methods=['GET'])
def average():
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

        return jsonify({
            "aeroporto_selezionato": airport,
            "numero_di_giorni_analizzati": numberOfDays,
            "numero_partenze": departures_count,
            "numero_di_arrivi": arrivals_count,
            "media_giornaliera_voli_in_partenza": round(average_departures, 2),
            "media_giornaliera_voli_in_arrivo": round(average_arrivals, 2)
        }), 200

    except Exception as e:
        return jsonify({
            "error": "SQL error",
            "details": str(e)
        }), 500


if __name__ == '__main__':
    port = int(os.environ.get('DATA_COLLECTOR_PORT', 5000))
    app.run(host='0.0.0.0', port=port)