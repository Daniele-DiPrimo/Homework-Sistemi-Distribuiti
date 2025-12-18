from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
import json
import logging

logging.basicConfig(level=logging.INFO)

# Consumer configuration
consumer_config = {
    'bootstrap.servers': 'broker-kafka:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,  # Manual commit for batch control
    'max.poll.interval.ms': 300000,  # 5 minutes timeout (important for batch processing)
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

def send_to_notify_system(messages):
    """
    Process a batch of messages and calculate aggregates.
    """
    if not messages:
        return
    
    logging.info(f"Message: {messages} has arrived on send_notify_system")
    logging.info(f"\n ...Sending to notify system...")

    ##QUI TUTTA LA LOGICA DI CONTROLLO SU HIGH_VALUE E LOW_VALUE. MANDA A NOTIFY SYSTEM SOLO "MANDA MAIL A QUELLO PERCHE' HA SUPERATO HIGH VALUE."

    for message in messages: 
        airports_count = message.get('airports_count', {}) #CONTIENE {'KLAX' : 4 , "LFPG" : 7}
        interests_list = message.get('interests', []) # CONTIENE [{'icao' : 'KLAX', 'high_value' = 10, 'low_value' = 2 } , ...  ]

        for interest in interests_list:
            try:
                user_email = interest.get('email')
                target_icao = interest.get('icao')  
                #CONTROLLO DI ESISTENZA!!!???
                high_threshold = interest.get('high_value')
                low_threshold = interest.get('low_value')

                if target_icao in airports_count: 
                    current_count = airports_count[target_icao]
                    logging.info(f"...ALERT-SYSTEM... I'M READY TO SEND A MESSAGE TO NOTIFIER ... DATA --> USER: {user_email}  ... INTEREST ICAO: {target_icao} ... HIGH_THRESHOLD FOR THIS ICAO: {high_threshold} ... LOW_THRESHOLD FOR THIS ICAO: {low_threshold} ... NUMBERO OF FLIGHT FOR THIS AIRPORT: {current_count}")
                    
                    if current_count > high_threshold: 
                        logging.info("curren_count > threshold")
                        message_to_notifier = {
                            "email": user_email, 
                            "current_icao": target_icao,
                            "number_of_flights":  current_count,
                            "high_threshold": high_threshold,
                            "type": "HIGH_LIMIT"
                        }                      
                    elif current_count < low_threshold:
                        logging.info("current_count < threshold")
                        message_to_notifier = {
                            "email": user_email, 
                            "current_icao": target_icao,
                            "number_of_flights":  current_count,
                            "low_threshold": low_threshold,
                            "type": "HIGH_LIMIT"
                        }
                    else: continue
                    
                    producer.produce(
                        topic2, 
                        json.dumps(message_to_notifier).encode('utf-8'),  # Explicit encoding
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
    logging.info("Waiting for messages...\n")
    
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
                send_to_notify_system(received_messages)
                
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
    logging.info("\nConsumer interrupted by user.")
finally:
    # Process remaining messages in buffer before shutdown
    if received_messages:
        logging.info("\nProcessing remaining messages before shutdown...")
        send_to_notify_system(received_messages)
        consumer.commit(asynchronous=False)
        producer.flush()
    
    logging.info("Closing consumer...")
    consumer.close()
    logging.info("Shutdown complete")


