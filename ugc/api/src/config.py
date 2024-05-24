import logging

from pydantic_settings import BaseSettings


class KafkaSet(BaseSettings):
    KAFKA_TOPIC: str
    KAFKA_HOST: str
    KAFKA_PORT: int
    GROUP_ID: str
    CONSUMER_TIMEOUT_MS: int
    MAX_RECORDS_PER_CONSUMER: int

    class Config:
        case_sensitive = False
        env_file = ".env"

settings = KafkaSet()

logger = logging.getLogger(__name__)