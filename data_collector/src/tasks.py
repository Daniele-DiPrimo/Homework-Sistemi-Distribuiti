"""Task e helper per il data collector.
Contiene funzioni per:
- ottenere token da OpenSky
- interrogare le API per i voli
- normalizzare i dati e inserirli nel DB
- inviare notifiche aggregate a Kafka
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import extensions
import logging
import requests
import os
import time
import json
from models import AirportsOfInterest, Flights
from sqlalchemy import insert
from datetime import datetime
from circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
import redis

logger = logging.getLogger(__name__)

# --- Redis Cache for flights data ---
flights_cache = redis.Redis(
    host=os.getenv('REDIS_HOST', 'data-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=1,
    decode_responses=True,
)

# --- Circuit Breaker for OpenSky API calls ---
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5)


def get_opensky_token():
    """Ottiene un token di accesso dall'API OpenSky usando client credentials."""

    token = flights_cache.get('opensky_token')
    if token:
        return token
    
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    secrets_path = os.getenv('OPENSKY_SECRET_PATH', '')

    with open(secrets_path, 'r') as f:
        config = json.load(f)

    client_id = config['clientId']
    client_secret = config['clientSecret']

    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }

    response = requests.post(url, data=payload, timeout=10)
    response.raise_for_status()

    data = response.json()
    token = data.get('access_token')
    expires_in = data.get('expires_in', 1800)
    
    flights_cache.setex('opensky_token', expires_in - 60, token)
    return token


def get_flights_by_airport(icao, begin, end, token, departure=None, arrival=None):
    """Chiama l'API opensky per ottenere i voli di andata/ritorno per un aeroporto."""

    departures_url = 'https://opensky-network.org/api/flights/departure'
    arrivals_url = 'https://opensky-network.org/api/flights/arrival'

    if departure and not arrival:
        url = departures_url
    elif arrival and not departure:
        url = arrivals_url
    else:
        deps = get_flights_by_airport(icao, begin, end, token, departure=True)
        arrs = get_flights_by_airport(icao, begin, end, token, arrival=True)
        return deps + arrs

    headers = {'Authorization': f'Bearer {token}'}
    params = {'airport': icao, 'begin': begin, 'end': end}

    response = requests.get(url, params=params, headers=headers, timeout=15)
    if response.status_code == 404:
        return []
    response.raise_for_status()
    return response.json()

def _clean_flights(raw_list):
    """Pulisce i dati grezzi (da API o Cache) per renderli compatibili col DB."""

    clean_result = []
    expected_columns = Flights.__table__.columns.keys()
    
    for r in raw_list:
        if r.get('estDepartureAirport') and r.get('estArrivalAirport'):
            # DB columns filtering
            flight = {k: v for k, v in r.items() if k in expected_columns}
            
            # Timestamp conversion
            if isinstance(flight.get('firstSeen'), (int, float)):
                flight['firstSeen'] = datetime.fromtimestamp(flight['firstSeen'])
            if isinstance(flight.get('lastSeen'), (int, float)):
                flight['lastSeen'] = datetime.fromtimestamp(flight['lastSeen'])
            
            clean_result.append(flight)
    return clean_result

def fetch_data(icao):
    """Recupera i voli per `icao."""

    cached_data = flights_cache.get(icao)
    if cached_data:
        logger.info(f"Cache hit for {icao}.")
        return _clean_flights(json.loads(cached_data))

    token = circuit_breaker.call(get_opensky_token)
    end = int(time.time())
    begin = end - 28800  # 8 hrs

    result = None
    try:
        result = circuit_breaker.call(get_flights_by_airport, icao, begin, end, token, departure=True, arrival=True)
        
        if result:
            flights_cache.setex(icao, 900, json.dumps(result))
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during API call for {icao}: {e}")

    if not result:
        return []

    return _clean_flights(result)


def send_to_kafka(message, email):
    """Invia la lista di interessi aggregati al topic `to-alert-system`."""

    if not message:
        logger.info("No flights to send.")
        return

    payload = {'email': email, 'interests': message}
    extensions.kafka_producer.send('to-alert-system', payload)
    logger.info("Sent aggregated statistics to Kafka.")


def update_database():
    """Funzione di aggiornamento del db Flights, invocata dal CRONJOB."""
    logger.info("--- Updating database... ---")
    try:
        # Take all interests from DB
        stmt = extensions.db.select(AirportsOfInterest)
        all_interests_orm = extensions.db.session.execute(stmt).scalars().all()
            
        if not all_interests_orm:
            logger.info("--- No interests found in DB. ---")
            return

        # Orm to List[Dict] for easier handling
        all_interests_dicts = [i.to_dict() for i in all_interests_orm]

        # Take unique ICAOs
        unique_icaos = {i['icao'] for i in all_interests_dicts}
        logger.info(f"Unique airports to fetch: {len(unique_icaos)}")

        # Temporary storage
        icao_counts_map = {} 
        all_flights_to_insert = []

        # Parallel fetching with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_icao = {
                executor.submit(fetch_data, icao): icao 
                for icao in unique_icaos
            }

            for future in as_completed(future_to_icao):
                icao = future_to_icao[future]
                try:
                    # result() to take care of exceptions raised in fetch_data
                    flights = future.result()
                        
                    icao_counts_map[icao] = len(flights)
                    if flights:
                        all_flights_to_insert.extend(flights)

                # Error handling
                except CircuitBreakerOpenException:
                    logger.warning(f"Skipping {icao}: Circuit Breaker is OPEN.")
                    icao_counts_map[icao] = 0
                    return False
                        
                except FileNotFoundError:
                    logger.critical(f"Failed {icao}: Secrets file not found!")
                    icao_counts_map[icao] = 0
                    return False
                        
                except Exception as e:
                    logger.error(f"Generic error fetching {icao}: {e}")
                    icao_counts_map[icao] = 0

        if all_flights_to_insert:
            logger.info(f"Inserting {len(all_flights_to_insert)} flights into DB...")
            stmt = insert(Flights).values(all_flights_to_insert)
            stmt = stmt.prefix_with('IGNORE')
            extensions.db.session.execute(stmt)
            extensions.db.session.commit()

        # Kafka distribution for user
        users_emails = {i['email'] for i in all_interests_dicts}
            
        for email in users_emails:
            # Users's interests filtering
            user_payload = [x for x in all_interests_dicts if x['email'] == email]
                
            # Fill data
            for item in user_payload:
                item['flights_count'] = icao_counts_map.get(item['icao'], 0)
                item.pop('email', None)

            # Send to Kafka data for sending one alert to user about 1 or more thresholds exceeded (1 email per user)
            send_to_kafka(user_payload, email)
            # Flush to avoid message loss after temporary pod death
            extensions.kafka_producer.flush()

        logger.info("--- Update done successfully. ---")

        return True

    except Exception as e:
        # General Rollback on critical error
        extensions.db.session.rollback()
        logger.error(f"Critical error in update task: {e}")
        return False

    
