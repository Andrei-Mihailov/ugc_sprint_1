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
from mapping import event_mappings


collect_dict = {}


def kafka_json_deserializer(serialized):
    return json.loads(serialized)


@on_exception(expo, (ConnectionError), max_tries=5)
def init_db():
    clickhouse_client = Client(
        host=settings.CH_HOST,
        database=settings.CH_DATABASE,
        user=settings.CH_USER,
        password=settings.CH_PASSWORD
    )
    # Создание таблиц, если они не существуют
    for item_dt in event_mappings.keys():
        attr_name = event_mappings[item_dt][0]
        attr_type = event_mappings[item_dt][1]
        order_by = event_mappings[item_dt][2]
        attr_str = ""
        for i in range(len(attr_name)):
            attr_str = attr_str + attr_name[i] + " " + attr_type[i]
            if i < len(attr_name) - 1:
                attr_str = attr_str + ","

        query =  f"""
            CREATE TABLE IF NOT EXISTS {settings.CH_DATABASE}.{item_dt}
                (
                    {attr_str}
                )
                Engine=MergeTree()
            ORDER BY ({",".join(order_by)})
            """
        print(query)
        clickhouse_client.execute(query)
        collect_dict[item_dt] = []
    
    clickhouse_client.disconnect()


@on_exception(expo, (KafkaConnectionError, NoBrokersAvailable, ConnectionError), max_tries=5)
def load_data_to_clickhouse():
    consumer = KafkaConsumer(
        settings.KAFKA_TOPIC,
        group_id=settings.KAFKA_GROUP,
        bootstrap_servers=[f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"],
        auto_offset_reset="earliest",
        value_deserializer=kafka_json_deserializer,
    )

    try:
        for message in consumer:
            message_key_str = message.key.decode("utf-8")

            if message_key_str in event_mappings.keys():
                payload = message.value
                payload_required = event_mappings[message_key_str][0]
                payload_required_types = event_mappings[message_key_str][1]
                checked = True

                for item in payload_required:
                    if not item in payload.keys():
                        checked = False
                        logger.error(f"Error when trying parse click event {payload}")

                if checked:
                    data_tuple = ()
                    for i in range(len(payload_required)): 
                        if payload_required_types[i] == "DateTime":
                            try:
                                date_column = datetime.strptime(payload[payload_required[i]], "%Y-%m-%d %H:%M:%S")
                                data_tuple += (date_column, )
                            except:
                                logger.error(f"Error when trying parse date {payload[payload_required[i]]}")
                        else:
                            data_tuple += (payload[payload_required[i]], )

                    exists_data = collect_dict[message_key_str]
                    exists_data.append(data_tuple)
                                          
                    if len(exists_data) >= settings.MAX_RECORDS_PER_CONSUMER:
                        clickhouse_client = Client(
                                                host=settings.CH_HOST,
                                                database="movies_analysis",
                                                user=settings.CH_USER,
                                                password=settings.CH_PASSWORD
                                            )
                        try:
                            query = f"INSERT INTO {message_key_str} ({','.join(payload_required)}) VALUES"
                            clickhouse_client.execute(query,
                                                      exists_data,
                            )
                            collect_dict[message_key_str] = []
                        except Exception as e:
                            logger.error(f"Error when trying to write Clickhouse: {str(e)}")
                            raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))
                        finally:
                            clickhouse_client.disconnect()
                    else:
                        collect_dict[message_key_str] = exists_data

                # TODO: будут теряться записи, если наша пачка не набралась, а случилось исключение
                # надо подумать, как обработать и записать их
    except Exception as e:
        logger.error(f"Error when trying to consume: {str(e)}")
        raise HTTPException(status_code=HTTPStatus.BAD_REQUEST, detail=str(e))
    finally:
        consumer.close()


if __name__ == "__main__":
    init_db()
    load_data_to_clickhouse()
