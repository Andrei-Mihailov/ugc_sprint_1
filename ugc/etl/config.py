import logging

from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ClickHouse
    CH_HOST: str = Field("localhost", env="CH_HOST")
    CH_PORT: int = Field(8123, env="CH_PORT")
    CH_USER: str = Field("localhost", env="CH_USER")
    CH_PASSWORD: str = Field(8123, env="CH_PASSWORD")
    CH_DATABASE: str = Field("movies_analysis", env="CH_DATABASE")
    # Kafka
    KAFKA_TOPIC: str = Field("events", env="KAFKA_TOPIC")
    KAFKA_HOST: str = Field("localhost", env="KAFKA_HOST")
    KAFKA_PORT: int = Field(9092, env="KAFKA_PORT")
    KAFKA_GROUP: str = Field("echo-messages", env="KAFKA_GROUP")
    CONSUMER_TIMEOUT_MS: int = Field(100, env="CONSUMER_TIMEOUT_MS")
    MAX_RECORDS_PER_CONSUMER: int = Field(100, env="MAX_RECORDS_PER_CONSUMER")

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

logger = logging.getLogger(__name__)
