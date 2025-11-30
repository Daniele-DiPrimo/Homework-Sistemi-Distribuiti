from extensions import db, scheduler
import logging
import requests
import os
import time
import json
from models import AirportsOfInterest, Flights
from sqlalchemy import insert
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_opensky_token():
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    
    secrets_path = os.getenv('SECRETS_PATH', '')

    try:
        with open(secrets_path, 'r') as f:
            config = json.load(f)
        
        client_id = config['clientId']
        client_secret = config['clientSecret']

    except FileNotFoundError:
        logger.error("Secrets not found!")
        return None

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    try: 
        # data=payload set header at 'Content-Type: application/x-www-form-urlencoded'
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")
        return access_token
    except requests.exceptions.RequestException as e:
        logger.error(f"Error retrieving token: {e}")
        return None

def get_flights_by_airport(icao, begin, end, token, departure=None, arrival=None):
    departures_url = "https://opensky-network.org/api/flights/departure"
    arrivals_url = "https://opensky-network.org/api/flights/arrival"

    if departure and not arrival:
        url = departures_url
    elif not departure and arrival:
        url = arrivals_url
    else:
        deps = get_flights_by_airport(icao, begin, end, token, departure=True)
        arrs = get_flights_by_airport(icao, begin, end, token, arrival=True)
        return deps + arrs

    headers = {}
    headers['Authorization'] = f"Bearer {token}"

    payload = {
        "airport": icao,
        "begin": begin,
        "end": end
    }

    try:
        response = requests.get(url, params=payload, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error during api call: {e}")
        return []


@scheduler.task('interval', id='update_db', seconds=10)
def update_database():
    with scheduler.app.app_context():
        logger.info("--- Update database. ---")
        
        #read airports from flights_db
        stmt = db.select(AirportsOfInterest.icao)
        result = db.session.execute(stmt) 
        airports_icao = result.scalars().all()

        if not airports_icao:
            logger.info("--- No airports found in DB. ---")
            return
        
        #retrieving token
        token = get_opensky_token()

        if not token:
            logger.error("Error retrieving token!")
            return
        
        #retrieving info on flights for specified airports
        result = []

        for icao in airports_icao:
            end = int(time.time())
            begin = end - 86400
            result += get_flights_by_airport(icao, begin, end, token, departure=True, arrival=True)

        #logger.info(f"Received data:\n{json.dumps(result, indent=2)}")

        if not result:
            logger.error("--- No results found. ---")
            return

        #cleaning and filtering results
        clean_result = []

        for r in result:
            if r.get('estDepartureAirport') and r.get('estArrivalAirport'):
                flight = {k: v for k, v in r.items() if k in Flights.__table__.columns.keys()}
                
                flight['firstSeen'] = datetime.fromtimestamp(flight['firstSeen']) 
                flight['lastSeen'] = datetime.fromtimestamp(flight['lastSeen'])
                    
                clean_result.append(flight)

        #save info in flights_db 
        db.session.execute(insert(Flights), clean_result)
        db.session.commit()

        logger.info("--- Update done. ---")