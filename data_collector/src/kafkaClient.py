"""Semplice wrapper per il produttore Kafka (confluent_kafka)."""

from confluent_kafka import Producer
import json
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class KafkaProducer:
    """Wrapper leggero attorno a `confluent_kafka.Producer`.

    Il metodo `send` serializza il messaggio in JSON e lo produce sul topic
    indicato. Usa un callback per report di delivery.
    """

    def __init__(self, config):
        try:
            self.producer = Producer(config)
            logger.info("Kafka Producer initialized.")
        except Exception as e:
            logger.error(f"Failed to initialize producer: {e}")

    def delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def send(self, topic, message):
        """Invia `message` (serializzato come JSON) al `topic` specificato."""
        self.producer.produce(topic, json.dumps(message).encode('utf-8'), callback=self.delivery_report)
        logger.debug(f"Produced message to topic {topic}: {message}")
        # Poll to trigger delivery report callbacks
        self.producer.poll(0)

    def flush(self):
        """Attende che tutti i messaggi in coda siano inviati."""
        self.producer.flush()


