import logging

from pydantic import Field
from pydantic_settings import BaseSettings
from kafka3 import KafkaProducer


class Settings(BaseSettings):
    # Kafka
    KAFKA_TOPIC: str = Field("events", env="KAFKA_TOPIC")
    KAFKA_HOST: str = Field("localhost", env="KAFKA_HOST")
    KAFKA_PORT: int = Field(9092, env="KAFKA_PORT")
    KAFKA_GROUP: str = Field("echo-messages", env="KAFKA_GROUP")
    CONSUMER_TIMEOUT_MS: int = Field(100, env="CONSUMER_TIMEOUT_MS")
    MAX_RECORDS_PER_CONSUMER: int = Field(100, env="MAX_RECORDS_PER_CONSUMER")
    NUM_PARTITIONS: int = Field(1, env="NUM_PARTITIONS")
    REPLICATION_FACTOR: int = Field(1, env="REPLICATION_FACTOR")
    JWT_SECRET_KEY: str = Field(env="JWT_SECRET_KEY", default="secret-key")
    MAX_TRIES: int = Field(env="MAX_TRIES", default=5)
    DEBUG: bool = Field(env="DEBUG", default=True)

    AUTH_API_ME_URL: str

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# producer = KafkaProducer(
#     bootstrap_servers=f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"
# )

# logger.info("Продюсер Kafka успешно создан.")
