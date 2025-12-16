import json
import logging
from kafka import KafkaProducer

logger = logging.getLogger(__name__)

# Variabile globale che conterrà l'istanza
producer = None

def init_kafka_producer():
    """
    Inizializza il producer. 
    Da chiamare UNA SOLA VOLTA all'avvio dell'app.
    """
    global producer
    if producer is not None:
        logger.warning("Kafka Producer già inizializzato!")
        return

    try:
        print("Tentativo di connessione a Kafka...")
        producer = KafkaProducer(
            bootstrap_servers=['broker-kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Acks='all' garantisce che il messaggio sia salvato in modo sicuro
            acks='all',
            retries=3
        )
        print("Kafka Producer connesso con successo!")
    except Exception as e:
        logger.error(f"Errore critico connessione Kafka: {e}")
        producer = None

def get_producer():
    """Restituisce l'istanza globale."""
    return producer