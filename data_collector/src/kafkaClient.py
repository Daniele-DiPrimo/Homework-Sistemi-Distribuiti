from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)

class KafkaProducer: 

    def __init__(self, config):
        try:
            self.producer = Producer(config)
            logger.info("Kafka Producer initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")
    
    def delivery_report(self, err, msg): 

        if err: 
            print(f"Delivery failed: {err}")
        else: 
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send(self, topic, message):
        self.producer.produce(
            topic,
            json.dumps(message).encode('utf-8'),
            callback = self.delivery_report
        )

        print(f"this message: {message} has send in topic {topic}")

        self.producer.poll(0)
        self.producer.flush()

