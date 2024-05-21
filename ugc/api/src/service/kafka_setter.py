import asyncio
import json
import logging
from http import HTTPStatus
from kafka import KafkaConsumer, KafkaProducer
from flask import Flask, jsonify

from models.models import UserValues
from config import settings

logger = logging.getLogger(__name__)

# Инициализируем цикл событий asyncio
loop = asyncio.get_event_loop()

# Создаем приложение Flask
app = Flask(__name__)

# Функция десериализации JSON для Kafka сообщений
def kafka_json_deserializer(serialized):
    return json.loads(serialized)

# Функция для отправки сообщения в Kafka
async def process_load_kafka(value):
    producer = KafkaProducer(bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'])
    try:
        # Отправляем сообщение
        producer.send(settings.KAFKA_TOPIC, value=value)
    except Exception as exc:
        logger.exception(exc)
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        producer.close()
        return {}

# Функция для получения сообщений из Kafka
async def process_get_messages():
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

# Маршрут для отправки сообщений в Kafka
@app.route('/send', methods=['POST'])
def send_message_to_kafka():
    data = request.json
    if not data:
        return jsonify({"error": "Data is required"}), HTTPStatus.BAD_REQUEST

    try:
        asyncio.run(process_load_kafka(json.dumps(data).encode()))
        return jsonify({"status": "Message sent to Kafka"}), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

# Маршрут для получения сообщений из Kafka
@app.route('/receive', methods=['GET'])
def receive_messages_from_kafka():
    try:
        messages = asyncio.run(process_get_messages())
        return jsonify([message.dict() for message in messages]), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

if __name__ == '__main__':
    app.run()
