import logging
from typing import Union

from pydantic import Field
from pydantic_settings import BaseSettings
from kafka3 import KafkaProducer, KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


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

    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def create_kafka_topic():
    admin_client = KafkaAdminClient(
        bootstrap_servers=f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}",
        client_id="admin_client",
    )

    topic_list = [
        NewTopic(
            name=settings.KAFKA_TOPIC,
            num_partitions=settings.NUM_PARTITIONS,
            replication_factor=settings.REPLICATION_FACTOR,
        )
    ]

    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        logger.info(
            f"Тема '{settings.KAFKA_TOPIC}' создана с {settings.NUM_PARTITIONS} партициями и фактором репликации {settings.REPLICATION_FACTOR}."
        )
    except TopicAlreadyExistsError:
        logger.warning(f"Тема '{settings.KAFKA_TOPIC}' уже существует.")
    except Exception as e:
        logger.error(f"Не удалось создать тему '{settings.KAFKA_TOPIC}': {e}")
    finally:
        admin_client.close()


# Создание темы Kafka
create_kafka_topic()

# Пример создания продюсера
producer = KafkaProducer(
    bootstrap_servers=f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"
)

logger.info("Продюсер Kafka успешно создан.")
