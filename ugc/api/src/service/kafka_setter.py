from http.client import HTTPException
import json
from http import HTTPStatus

from kafka3 import KafkaConsumer, KafkaProducer
from kafka3.admin import KafkaAdminClient, NewTopic
from kafka3.errors import KafkaConnectionError
from backoff import on_exception, expo

from models.models import UserValues
from config import settings, logger

# TODO: backoff проверить, количество попыток вынести в env\конфиг
# TODO: партицирование  и репликацию доделать, вынести в env\конфиг
@on_exception(expo, (KafkaConnectionError), max_tries=5)
def kafka_check_topic(name_topic: str):   
    admin_client = KafkaAdminClient(
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'],
        client_id='ugc'
    )
    server_topics = admin_client.list_topics()
    if not name_topic in server_topics:
        new_topic_list = []
        new_topic_list.append(NewTopic(name=name_topic, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=new_topic_list, validate_only=False)
    

# TODO: backoff
@on_exception(expo, (KafkaConnectionError), max_tries=5)
def process_load_kafka(value):
    kafka_check_topic(settings.KAFKA_TOPIC)
    producer = KafkaProducer(bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'],
                             client_id='ugc')
    try:
        producer.send(settings.KAFKA_TOPIC, value=value)
    except Exception as exc:
        logger.exception(exc)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail=str(exc))
    finally:
        producer.close()
        return {}


def kafka_json_deserializer(serialized):
    return json.loads(serialized)


# для тестирования. не проверяла работу
# в etl процесс получения данных реализован
# TODO: backoff
def process_get_messages():
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        group_id=settings.GROUP_ID,
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'],
        auto_offset_reset="earliest",
        value_deserializer=kafka_json_deserializer,
    )

    retrieved_requests = []
    try:
        for message in consumer:
            retrieved_requests.append(UserValues(value=json.dumps(message.value)))

            if len(retrieved_requests) >= settings.MAX_RECORDS_PER_CONSUMER:
                break

    except Exception as e:
        logger.error(f"Error when trying to consume: {str(e)}")
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))
    finally:
        consumer.close()

    return retrieved_requests
