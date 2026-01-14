"""
Alert Notifier Service

Questo modulo si iscrive al topic Kafka `to-notify` e invia email
agli utenti quando vengono segnalati superamenti di soglie.
Il codice è volutamente minimale: usa `confluent_kafka.Consumer`
per leggere messaggi e `smtplib` per l'invio delle email.
"""

from confluent_kafka import Consumer, KafkaError
import json
import logging
import smtplib
import ssl
from email.message import EmailMessage
import os

logging.basicConfig(level=logging.INFO)

# SMTP configuration (possibile sovrascriverle con variabili d'ambiente)
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '465'))
SENDER_EMAIL = os.getenv('SENDER_EMAIL', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'broker-kafka:9092',
    'group.id': 'group2',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
}

consumer = Consumer(consumer_config)
TOPIC = 'to-notify'

# Dimensione del batch (qui 1 per semplicità)
BATCH_SIZE = 1
received_messages = []
message_count = 0

consumer.subscribe([TOPIC])


def send_email(notification: dict) -> bool:
    """Costruisce e invia una email a `notification['email']`.

    La funzione legge gli interessi (soglie superate) e costruisce
    il corpo del messaggio. Se le credenziali SMTP non sono impostate
    viene loggato l'errore e si ritorna False.
    """
    if not SENDER_EMAIL or not EMAIL_PASSWORD:
        logging.error("Sender email or password not set in environment variables.")
        return False

    msg = EmailMessage()
    msg['Subject'] = "NOTIFICATION: flights threshold exceeded"
    msg['From'] = SENDER_EMAIL
    msg['To'] = notification.get('email')

    interests = notification.get('interests', [])
    if interests:
        lines = []
        for it in interests:
            # ogni interesse contiene le informazioni rilevanti
            lines.append(
                f"Airport {it.get('current_icao')}: {it.get('number_of_flights')} flights "
                f"(threshold {it.get('threshold')} - {it.get('type')})"
            )
        lines.append("Best regards.")
        msg.set_content("\n".join(lines))
    else:
        msg.set_content("No interesting events to notify.")

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.send_message(msg)
        logging.info(f"Email inviata a {notification.get('email')}")
        return True
    except Exception as e:
        logging.error(f"Error sending email: {e}")
        return False


def _process_message_batch(messages: list):
    """Processa una lista di messaggi e invia le email corrispondenti."""
    for m in messages:
        send_email(m)


def main_loop():
    logging.info(f"Consumer ALERT-NOTIFIER-SYSTEM started. Batch size: {BATCH_SIZE}")
    logging.info("Waiting for messages...")

    try:
        global message_count, received_messages
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                # EOF or other errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.debug(f"End of partition {msg.partition()}")
                else:
                    logging.error(f"Consumer error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                received_messages.append(data)
                message_count += 1
                logging.info(f"Received message #{message_count} (batch {message_count}/{BATCH_SIZE})")

                if message_count >= BATCH_SIZE:
                    logging.info(f"Processing batch of {len(received_messages)} messages")
                    _process_message_batch(received_messages)

                    # Commit offsets dopo il processamento per at-least-once
                    consumer.commit(asynchronous=False)
                    logging.info(f"Committed offset: {msg.offset()}")

                    # reset batch
                    received_messages = []
                    message_count = 0

            except (json.JSONDecodeError, KeyError) as e:
                logging.error(f"Malformed message at offset {msg.offset()}: {e}")
                # Commit malformed message offset per evitare reprocessing infinito
                consumer.commit(msg)
                continue

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user.")
    finally:
        # Process remaining messages before shutdown
        if received_messages:
            logging.info("Processing remaining messages before shutdown...")
            _process_message_batch(received_messages)
            consumer.commit(asynchronous=False)

        logging.info("Closing consumer...")
        consumer.close()
        logging.info("Shutdown complete")


if __name__ == '__main__':
    main_loop()