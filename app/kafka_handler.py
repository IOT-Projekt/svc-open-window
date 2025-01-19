import os
import logging
from typing import List
from kafka import KafkaConsumer, KafkaProducer
import sys
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class KafkaConfig:
    """Encapsulates Kafka configuration using Singleton pattern."""

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(KafkaConfig, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        """Initialize configuration values. Just set variables like Kafka broker IP from the .env file."""
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )  # Kafka broker(s)
        self.group_id = os.getenv("KAFKA_GROUP_ID", "fake-consumer-group")
        self.validate()

    def validate(self) -> bool:
        """Validate if required configuration values are set."""
        if not self.bootstrap_servers:
            raise ValueError(
                "Environment variable KAFKA_BOOTSTRAP_SERVERS is missing or empty."
            )
        if not self.group_id:
            raise ValueError("Environment variable KAFKA_GROUP_ID is missing or empty.")


def setup_kafka_consumer(config: KafkaConfig, topics: List[str]) -> KafkaConsumer:
    """Sets up a default Kafka consumer. Topics are passed as a list of strings."""
    consumer = KafkaConsumer(
        *topics,
        group_id=config.group_id,
        bootstrap_servers=config.bootstrap_servers,
        value_deserializer=json_deserializer,
    )
    return consumer


def setup_kafka_producer(config: KafkaConfig) -> KafkaProducer:
    """Sets up a default Kafka producer."""
    producer = KafkaProducer(
        bootstrap_servers=config.bootstrap_servers, 
        value_serializer=lambda v: v.encode("utf-8"),
    )
    return producer


def json_deserializer(message):
    """Deserialize JSON message."""
    try:
        return json.loads(message.decode("utf-8"))
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON message: {e}")
        return None

def on_message_print(msg) -> None:
    """Process and print the received Kafka message."""
    logging.info(f"Received message: {msg.topic} -> {msg.value.decode('utf-8')}")


def close_consumer(consumer: KafkaConsumer) -> None:
    """Method to close the kafka consumer connection."""
    logging.info("Closing Kafka consumer")
    consumer.close()
    sys.exit(0)


def close_producer(producer: KafkaProducer) -> None:
    """Method to close the kafka producer connection."""
    logging.info("Closing Kafka producer")
    producer.close()
    sys.exit(0)


def send_kafka_message(producer: KafkaProducer, topic: str, payload: str) -> None:
    """Method to send a message to a Kafka topic."""

    # always use this json layout in order to work with the mqtt-kafka-bridge
    json_content = {'message': json.dumps({'source': 'kafka','payload': payload})}
    
    producer.send(topic, json.dumps(json_content))
    logging.info(f"Sent message to Kafka topic: {topic}")
