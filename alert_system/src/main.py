"""
Alert System
Legge messaggi dal topic `to-alert-system`, valuta le soglie
e pubblica eventi su `to-notify` per le notifiche via email.
Il comportamento è: leggere, valutare interessi, produrre messaggi di notifica.
"""

import os
from confluent_kafka import Consumer, KafkaError, Producer
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# --- Kafka setup---
bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '')

if not bootstrap_servers:
    logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable not set.")

# --- Kafka Consumer & Producer setup ---
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'max.poll.interval.ms': 300000,
}

producer_config = {
    'bootstrap.servers': bootstrap_servers,
    'linger.ms': 100,
}

consumer = Consumer(consumer_config)
producer = Producer(producer_config)

# --- Topics ---
INPUT_TOPIC = 'to-alert-system'
NOTIFY_TOPIC = 'to-notify'

# --- General settings ---
BATCH_SIZE = 1
received_messages = []
message_count = 0

consumer.subscribe([INPUT_TOPIC])

def send_to_notify_system(message: dict):
    """Costruisce il payload per il sistema di notifica a partire da `message`.
    La funzione confronta il numero di voli registrati con le soglie
    (`high_value` e `low_value`) e inserisce nel payload solo gli
    interessi che hanno superato un limite.
    """
    if not message:
        return

    logger.info("Processing message for notify system")

    payload = {
        'email': message.get('email'),
        'interests': []
    }

    try:
        for interest in message.get('interests', []):
            icao = interest.get('icao')
            high_threshold = interest.get('high_value')
            low_threshold = interest.get('low_value')
            flights_count = interest.get('flights_count')

            # Adding only if thresholds are exceeded
            if high_threshold is not None and flights_count > high_threshold:
                payload['interests'].append({
                    'current_icao': icao,
                    'number_of_flights': flights_count,
                    'threshold': high_threshold,
                    'type': 'HIGH_LIMIT'
                })
            elif low_threshold is not None and flights_count < low_threshold:
                payload['interests'].append({
                    'current_icao': icao,
                    'number_of_flights': flights_count,
                    'threshold': low_threshold,
                    'type': 'LOW_LIMIT'
                })
            else:
                continue

        # Create Kafka message only if there are interests to notify
        if(payload['interests']):
            producer.produce(NOTIFY_TOPIC, json.dumps(payload).encode('utf-8'), callback=delivery_report)
            producer.poll(0)

    except KeyError as e:
        logger.error(f"Missing key in interest object: {e}")


def delivery_report(err, msg):
    """Callback per tracciare l'esito della consegna dei messaggi Kafka."""
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def main_loop():
    logger.info(f"Consumer started. Batch size: {BATCH_SIZE}")
    logger.info("Waiting for messages...")

    try:
        global message_count, received_messages
        while True:
            # Poll producer to handle delivery reports
            producer.poll(0)

            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(f"End of partition {msg.partition()}")
                else:
                    logger.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                received_messages.append(data)
                message_count += 1
                logger.info(f"Received message #{message_count} (batch {message_count}/{BATCH_SIZE})")

                if message_count >= BATCH_SIZE:
                    for message in received_messages:
                        send_to_notify_system(message)

                    # Commit offset to guarantee at-least-once
                    consumer.commit(asynchronous=False)
                    logger.info(f"Committed offset: {msg.offset()}")

                    received_messages = []
                    message_count = 0

            except (json.JSONDecodeError, KeyError) as e:
                logger.error(f"Malformed message at offset {msg.offset()}: {e}")
                consumer.commit(msg)
                continue

    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user.")
    finally:
        if received_messages:
            logger.info("Processing remaining messages before shutdown...")
            for message in received_messages:
                send_to_notify_system(message)

        consumer.commit(asynchronous=False)
        producer.flush()
        consumer.close()
        logger.info("Shutdown complete")


if __name__ == '__main__':
    main_loop()


