from kafka import KafkaProducer
import json
from src.config.settings import KAFKA_BOOTSTRAP_SERVER


def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
