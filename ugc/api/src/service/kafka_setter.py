from http.client import HTTPException
import json
from http import HTTPStatus

from kafka3 import KafkaProducer
from kafka3.errors import KafkaConnectionError
from backoff import on_exception, expo

from config import settings, logger


# TODO: backoff
@on_exception(expo, (KafkaConnectionError), max_tries=5)
def process_load_kafka(key, value):
    producer = KafkaProducer(
        bootstrap_servers=[f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"],
        client_id="ugc",
    )
    try:
        producer.send(settings.KAFKA_TOPIC, value=value, key=key)

    except Exception as exc:
        logger.exception(exc)
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=str(exc)
        )
    finally:
        producer.close()
