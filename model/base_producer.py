import json
import logging
import os

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
log = logging.getLogger(__name__)


class BaseProducer:
    def __init__(self, topic: str | None = None):
        registry_url = os.getenv("SCHEMA_REGISTRY_URL")
        if registry_url and topic:
            from market_data_models.registry import AvroSerializer
            self._serializer = AvroSerializer(topic, registry_url)
            log.info("Using Avro serialization (registry=%s, topic=%s)", registry_url, topic)
        else:
            self._serializer = lambda v: json.dumps(v).encode("utf-8")
            if not registry_url:
                log.info("SCHEMA_REGISTRY_URL not set — using JSON serialization")

        self._producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            value_serializer=self._serializer,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
        )

    def send(self, topic: str, value: dict, key: str = None):
        self._producer.send(topic, value=value, key=key)

    def flush(self):
        self._producer.flush()

    def close(self):
        self._producer.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
