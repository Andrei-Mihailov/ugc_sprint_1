from http import HTTPStatus
from http.client import HTTPException
from requests.exceptions import ConnectionError
import json
from datetime import datetime

from kafka3 import KafkaConsumer
from clickhouse_driver import Client
from kafka3.errors import KafkaConnectionError, NoBrokersAvailable
from backoff import on_exception, expo

from config import settings, logger


def kafka_json_deserializer(serialized):
    return json.loads(serialized)


@on_exception(expo, (ConnectionError), max_tries=5)
def init_db():   
    global clickhouse_client
    clickhouse_client = Client(host="clickhouse", database='movies_analysis', user='app', password='qwe123')
    # TODO: сделать маппинг бд и свойств
    clickhouse_client.execute(
        """
        CREATE TABLE IF NOT EXISTS movies_analysis.movies_table
            (
                user_id String,
                user_fio String,
                movie_id String,
                movie_name String,
                date_event DateTime,
                fully_viewed Bool
            )
            Engine=MergeTree()
        ORDER BY movie_timestamp
        """,
    )


@on_exception(expo, (KafkaConnectionError, NoBrokersAvailable), max_tries=5)
def load_data_to_clickhouse():

    global consumer
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        group_id=settings.KAFKA_GROUP,
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'],
        auto_offset_reset="earliest",
        value_deserializer=kafka_json_deserializer,
    )

    while True:
        retrieved_requests = []
        try:
            for message in consumer:
                # TODO: сделать маппинг события и его свойств
                if message.key == b'click':                  
                    payload = message.value
                    data = (payload['film_id'],
                            datetime.strptime(payload['current_date'], "%Y-%m-%d %H:%M:%S"),
                            payload['user_id'],
                            payload['user_fio'],
                            payload['movie_name'],
                            payload['fully_viewed'])
                    retrieved_requests.append(data)

                # TODO: будут теряться записи, если наша пачка не набралась, а случилось исключение
                # надо подумать, как обработать и записать их
                if len(retrieved_requests) >= settings.MAX_RECORDS_PER_CONSUMER:
                    break

        except Exception as e:
            logger.error(f"Error when trying to consume: {str(e)}")
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))
        
        try:
            clickhouse_client.execute("""INSERT INTO movies_table (movie_id,
                                                            date_event,
                                                            user_id,
                                                            user_fio,
                                                            movie_name,
                                                            fully_viewed                                                          
                                ) VALUES""", retrieved_requests)
        except Exception as e:
            logger.error(f"Error when trying to write Clickhouse: {str(e)}")
            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))

if __name__ == '__main__':
    try:
        init_db()
        load_data_to_clickhouse()
    finally:
        clickhouse_client.disconnect()
        consumer.close()

    

