from confluent_kafka import Consumer, KafkaError
import json 
import logging
import smtplib
import ssl
from email.message import EmailMessage
import os

logging.basicConfig(level=logging.INFO)

SMTP_SERVER = os.getenv('SMTP_SERVER', '')
SMTP_PORT = int(os.getenv('SMTP_PORT', ''))
SENDER_EMAIL = os.getenv('SENDER_EMAIL', '')
EMAIL_PASSWORD = os.getenv('EMAIL_PASSWORD', '')

consumer_config = {
    'bootstrap.servers': 'broker-kafka:9092',
    'group.id': 'group2', 
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}

consumer = Consumer(consumer_config)

topic = 'to-notify'

BATCH_SIZE = 1 
received_messages = []
message_count = 0

consumer.subscribe([topic])

def send_email(data):
    msg = EmailMessage()
    msg['Subject'] = f"NOTIFICATION: flights for/from {data.get('current_icao')} exceeded threshold, {data.get('type')}"
    msg['From'] = os.getenv('SENDER_EMAIL')
    msg['To'] = data.get('email')
    
    content = f"""
    The number of flights for/from {data.get('current_icao')} exceeded your threshold.
    There were {data.get('number_of_flights')} number of flights and your threshold is {data.get('threshold')}.
    Best regards.
    """
    msg.set_content(content)

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
            server.login(SENDER_EMAIL, EMAIL_PASSWORD)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

try:
    logging.info(f"Consumer ALERT-NOTIFIER-SYSTEM. Batch size: {BATCH_SIZE}")
    logging.info("Waiting for messages...")
    
    while True:
        # Poll for new messages
        msg = consumer.poll(1.0)
        
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"End of partition {msg.partition()}")
            else:
                print(f"Consumer error: {msg.error()}")
            continue
        
        # Parse message
        try:
            data = json.loads(msg.value().decode('utf-8'))
            received_messages.append(data)
            message_count += 1
            
            logging.info(f"Received message #{message_count} (batch progress: {message_count}/{BATCH_SIZE})")
            
            # Improvement: Commit only AFTER batch processing
            if message_count >= BATCH_SIZE:
                # First process the batch
                logging.info(f" message = {received_messages} received, ready to process....")

                for message in received_messages:
                    send_email(message)

                # Then commit offset (ensures at-least-once semantics)
                consumer.commit(asynchronous=False)
                logging.info(f"Committed offset: {msg.offset()}\n")
                
                # Reset for next batch
                received_messages = []
                message_count = 0
                
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Malformed message at offset {msg.offset()}: {e}")
            # Commit malformed messages to avoid reprocessing
            consumer.commit(msg)
            continue

except KeyboardInterrupt:
    logging.info("Consumer interrupted by user.")
finally:
    # Process remaining messages in buffer before shutdown
    if received_messages:
        logging.info("Processing remaining messages before shutdown...")

        for message in received_messages:
            send_email(message)
        
        consumer.commit(asynchronous=False)
    
    logging.info("Closing consumer...")
    consumer.close()
    logging.info("Shutdown complete")