from extensions import db, scheduler
import logging
import requests
import os
import time
import json
from models import AirportsOfInterest, Flights
from sqlalchemy import insert
from datetime import datetime
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException

logger = logging.getLogger(__name__)

circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5)

def get_opensky_token():
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    
    secrets_path = os.getenv('SECRETS_PATH', '')

    with open(secrets_path, 'r') as f:
        config = json.load(f)
        
    client_id = config['clientId']
    client_secret = config['clientSecret']

    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    # data=payload set header at 'Content-Type: application/x-www-form-urlencoded'
    response = requests.post(url, data=payload, timeout=10)
    response.raise_for_status()

    token_data = response.json()
    access_token = token_data.get("access_token")
    return access_token

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

    response = requests.get(url, params=payload, headers=headers, timeout=15)

    if response.status_code == 404:
        logger.info(f"Airport not supported or no data available for: {icao}")
        return []
    
    response.raise_for_status()
    return response.json()

def fetch_and_update_db(airports_icao):
    #retrieving token
    token = circuit_breaker.call(get_opensky_token)
        
    #retrieving info on flights for specified airports
    result = []
    end = int(time.time())
    begin = end - 86400

    for icao in airports_icao: 
        try:
            result += circuit_breaker.call(
                get_flights_by_airport,
                icao,
                begin,
                end,
                token,
                departure=True,
                arrival=True
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Error during API call for {icao}: {e}")
            continue

    if not result:
        raise Exception("No results found.")

    #cleaning and filtering results
    clean_result = []

    for r in result:
        if r.get('estDepartureAirport') and r.get('estArrivalAirport'):
            flight = {k: v for k, v in r.items() if k in Flights.__table__.columns.keys()}
                
            flight['firstSeen'] = datetime.fromtimestamp(flight['firstSeen']) 
            flight['lastSeen'] = datetime.fromtimestamp(flight['lastSeen'])
                    
            clean_result.append(flight)

    #save info in flights_db
    stmt = insert(Flights).values(clean_result)
    stmt = stmt.prefix_with('IGNORE')
    db.session.execute(stmt)
    db.session.commit()

@scheduler.task('interval', id='update_db', hours=24)
def update_database():
    with scheduler.app.app_context():
        logger.info("--- Updating database... ---")
        
        try: 
            #read airports from flights_db
            stmt = db.select(AirportsOfInterest.icao)
            result = db.session.execute(stmt)
            airports_icao = result.scalars().all()

            if not airports_icao:
                logger.info("--- No airports found in DB. ---")
                return
            
            fetch_and_update_db(airports_icao)
            logger.info("--- Update done. ---")

        except CircuitBreakerOpenException:
            logger.error("Circuit is open. Skipping call.")
        except FileNotFoundError:
            logger.error("Cannot receive token: Secrets not found!")
        except Exception as e:
            logger.error(f"Generic error: {e}")
