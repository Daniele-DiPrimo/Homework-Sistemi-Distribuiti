from confluent_kafka import Consumer, KafkaError
import json 
import logging

logging.basicConfig(level=logging.INFO)

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

try:
    logging.info(f"Consumer ALERT-NOTIFIER-SYSTEM. Batch size: {BATCH_SIZE}")
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
                logging.info(f" message = {received_messages} received, ready to process....")  ##QUI RIMANDA AD UNA FUNZIONE CHE MANDA L'EMAIL CON LE INFO RICEVUTE
                
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
        # send_to_notify_system(received_messages)
        consumer.commit(asynchronous=False)
    
    logging.info("Closing consumer...")
    consumer.close()
    logging.info("Shutdown complete")