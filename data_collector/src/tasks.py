"""Task e helper per il data collector.

Contiene funzioni per:
- ottenere token da OpenSky
- interrogare le API per i voli
- normalizzare i dati e inserirli nel DB
- inviare notifiche aggregate a Kafka
"""

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

# Redis cache per i risultati dei voli (ttl in secondi)
flights_cache = redis.Redis(
    host=os.getenv('REDIS_HOST', 'data-cache'),
    port=int(os.getenv('REDIS_PORT', 6379)),
    db=2,
    decode_responses=True,
)

# Circuit breaker per le chiamate esterne
circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=5)


def get_opensky_token():
    """Recupera un access token da OpenSky usando client credentials.

    I segreti (clientId/clientSecret) sono letti da un file JSON il cui
    path è fornito tramite la variabile d'ambiente `SECRETS_PATH`.
    """
    url = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
    secrets_path = os.getenv('SECRETS_PATH', '')

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

    token_data = response.json()
    return token_data.get('access_token')


def get_flights_by_airport(icao, begin, end, token, departure=None, arrival=None):
    """Chiama l'API opensky per ottenere i voli di andata/ritorno per un aeroporto."""
    departures_url = 'https://opensky-network.org/api/flights/departure'
    arrivals_url = 'https://opensky-network.org/api/flights/arrival'

    if departure and not arrival:
        url = departures_url
    elif arrival and not departure:
        url = arrivals_url
    else:
        # se non specificato, unisco partenze+arrivi
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


def fetch_data(icao):
    """Recupera i voli per `icao`, usando cache e circuit breaker.

    Restituisce una lista di record puliti pronti per essere inseriti nel DB.
    """
    cached_data = flights_cache.get(icao)
    if cached_data:
        data = json.loads(cached_data)
        logger.info(f"Cache hit for {icao}. Retrieved {len(data)} flights from cache.")
        return data

    # Recupero token protetto da circuit breaker
    token = circuit_breaker.call(get_opensky_token)

    end = int(time.time())
    begin = end - 28800  # 8 ore
    result = None

    try:
        result = circuit_breaker.call(get_flights_by_airport, icao, begin, end, token, departure=True, arrival=True)
        flights_cache.setex(icao, 28800, json.dumps(result))
    except requests.exceptions.RequestException as e:
        logger.error(f"Error during API call for {icao}: {e}")

    if not result:
        raise Exception("No results found.")

    # Filtra e normalizza i risultati: manteniamo solo le colonne usate dal modello Flights
    clean_result = []
    for r in result:
        if r.get('estDepartureAirport') and r.get('estArrivalAirport'):
            flight = {k: v for k, v in r.items() if k in Flights.__table__.columns.keys()}
            # converte i timestamp in datetime
            flight['firstSeen'] = datetime.fromtimestamp(flight['firstSeen'])
            flight['lastSeen'] = datetime.fromtimestamp(flight['lastSeen'])
            clean_result.append(flight)

    return clean_result


def send_to_kafka(message, email):
    """Invia la lista di interessi aggregati al topic `to-alert-system`."""
    if not message:
        logger.info("No flights to send.")
        return

    payload = {'email': email, 'interests': message}
    extensions.kafka_producer.send('to-alert-system', payload)
    logger.info("Sent aggregated statistics to Kafka.")


@extensions.scheduler.task('interval', id='update_db', hours=8)
def update_database():
    """Task schedulato che aggiorna il DB e invia statistiche per ogni utente."""
    with extensions.scheduler.app.app_context():
        logger.info("--- Updating database... ---")
        try:
            stmt = extensions.db.select(AirportsOfInterest)
            result = extensions.db.session.execute(stmt)
            interests = result.scalars().all()
            interests = [i.to_dict() for i in interests]

            if not interests:
                logger.info("--- No interests found in DB. ---")
                return

            users = list({i.get('email') for i in interests})
            for user in users:
                user_interests = [i for i in interests if i['email'] == user]
                for interest in user_interests:
                    result = fetch_data(interest['icao'])

                    # aggiorna DB con nuovi voli (IGNORE per evitare duplicati)
                    stmt = insert(Flights).values(result)
                    stmt = stmt.prefix_with('IGNORE')
                    extensions.db.session.execute(stmt)
                    extensions.db.session.commit()

                    interest['flights_count'] = len(result)

                # rimuovo l'email prima di inviare a Kafka
                for ui in user_interests:
                    ui.pop('email', None)

                send_to_kafka(user_interests, user)

            logger.info("--- Update done. ---")

        except CircuitBreakerOpenException:
            logger.error("Circuit is open. Skipping call.")
        except FileNotFoundError:
            logger.error("Cannot receive token: Secrets not found!")
        except Exception as e:
            logger.error(f"Generic error: {e}")
