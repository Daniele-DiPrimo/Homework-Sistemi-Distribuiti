from confluent_kafka import Producer
import json 
import time

producer_config = {
    'bootstrap.servers':'broker-kafka:9092',
    'acks':'all', 
    'batch.size': 10000, 
    'max.in.flight.requests.per.connection':1,
    'retries':3, 
    'linger.ms':100
}

class KafkaProducer: 

    def __init__(self, topic):
        self.producer = Producer(producer_config)
        self.topic = topic 
        print("PRODUCER CREATO CORRETTAMENTE")
    
    def delivery_report(self, err, msg): 

        if err: 
            print(f"Delivery failed: {err}")
        else: 
            print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send(self, message):
        self.producer.produce(
            self.topic,
            json.dumps(message).encode('utf-8'),
            callback = self.delivery_report
        )

        print(f"this message: {message} has send in topic {self.topic}")

        self.producer.poll(0)
        self.producer.flush()

