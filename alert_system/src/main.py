from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import json
import logging

logging.basicConfig(level=logging.INFO)

# Consumer configuration
consumer_config = {
    'bootstrap.servers': 'broker-kafka:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'max.poll.interval.ms': 300000,
}

producer_config = {
    'bootstrap.servers': 'broker-kafka:9092', 
    'linger.ms': 100
}


consumer = Consumer(consumer_config)
producer = Producer(producer_config)

topic = 'to-alert-system'
topic2 = 'to-notify'

# Batch parameters
BATCH_SIZE = 1
received_messages = []
message_count = 0

consumer.subscribe([topic])

def send_to_notify_system(message):
    """
    Process a batch of messages and calculate aggregates.
    """
    if not message:
        return
    
    logging.info(f"Message: {message} has arrived on send_notify_system")
    logging.info(f"\n ...Sending to notify system...")

    
    airports_count = message.get('airports_count', {})
    interests_list = message.get('interests', [])
    for interest in interests_list:
        try:
            user_email = interest.get('email')
            target_icao = interest.get('icao')  
    
            high_threshold = interest.get('high_value')
            low_threshold = interest.get('low_value')

            if target_icao in airports_count: 
                current_count = airports_count[target_icao]
                    
                if high_threshold is not None and current_count > high_threshold: 
                    message_to_notifier = {
                        "email": user_email, 
                        "current_icao": target_icao,
                        "number_of_flights":  current_count,
                        "threshold": high_threshold,
                        "type": "HIGH_LIMIT"
                    }                      
                elif low_threshold is not None and current_count < low_threshold:
                    message_to_notifier = {
                        "email": user_email, 
                        "current_icao": target_icao,
                        "number_of_flights":  current_count,
                        "threshold": low_threshold,
                        "type": "LOW_LIMIT"
                    }
                else: continue
                    
                producer.produce(
                    topic2, 
                    json.dumps(message_to_notifier).encode('utf-8'),
                    callback=delivery_report
                )
                producer.poll(0)
                        
        except KeyError as e: 
            logging.error(f"DATI MANCANTI ALL'INTERNO DELL'OGGETTO INTEREST: {e}")


def delivery_report(err, msg): 

        if err: 
            print(f"Delivery failed: {err}")
        else: 
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

try:
    logging.info(f"Consumer started. Batch size: {BATCH_SIZE}")
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
            
            if message_count >= BATCH_SIZE:
                # First process the batch
                for message in received_messages:
                    send_to_notify_system(message)
                
                # Then commit offset (ensures at-least-once semantics)
                consumer.commit(asynchronous=False)
                logging.info(f"Committed offset: {msg.offset()}")
                
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
            send_to_notify_system(message)

        consumer.commit(asynchronous=False)
        producer.flush()
    
    logging.info("Closing consumer...")
    consumer.close()
    logging.info("Shutdown complete")


