import logging
from typing import Union

from pydantic import Field
from pydantic_settings import BaseSettings
from kafka3 import KafkaProducer


class Settings(BaseSettings):
    # Kafka
    KAFKA_TOPIC: str = Field('events', env='KAFKA_TOPIC')
    KAFKA_HOST: str = Field('localhost', env='KAFKA_HOST')
    KAFKA_PORT: int = Field(9092, env='KAFKA_PORT')
    KAFKA_GROUP: str = Field('echo-messages', env='KAFKA_GROUP')
    CONSUMER_TIMEOUT_MS: int = Field(100, env='CONSUMER_TIMEOUT_MS')
    MAX_RECORDS_PER_CONSUMER: int = Field(100, env='MAX_RECORDS_PER_CONSUMER')

    class Config:
        env_file = '.env'
        case_sensitive = False


settings = Settings()

logger = logging.getLogger(__name__)
