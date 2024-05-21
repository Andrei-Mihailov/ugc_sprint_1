import logging
from flask import Flask, jsonify, Blueprint
from api.v1.kafka_producer import kafka_blueprint  # Предполагается, что у вас есть Blueprint в kafka_producer
from dotenv import load_dotenv
from pydantic import BaseSettings
from os import getenv
from routes.ugc import ugc_blueprint

# Загрузка переменных окружения из .env файла
load_dotenv()

class KafkaSet(BaseSettings):
    KAFKA_TOPIC: str = getenv('KAFKA_TOPIC', 'events')
    KAFKA_HOST: str = getenv('KAFKA_HOST', '127.0.0.1')
    KAFKA_PORT: int = getenv('KAFKA_PORT', 9092)
    GROUP_ID: str = "echo-messages"
    CONSUMER_TIMEOUT_MS: int = 100
    MAX_RECORDS_PER_CONSUMER: int = 100

    class Config:
        case_sensitive = False

settings = KafkaSet()

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config['APPLICATION_ROOT'] = '/ugc'

# Настройка кастомного класса ответа и других параметров, если необходимо
class CustomJSONResponse(jsonify):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.headers['Content-Type'] = 'application/json; charset=utf-8'

app.response_class = CustomJSONResponse

@app.route('/api/openapi', methods=['GET'])
def openapi_docs():
    return jsonify({
        "title": "UGC",
        "description": "Асинхронный сборщик UGC",
        "docs_url": "/api/openapi",
        "openapi_url": "/api/openapi.json",
    })

# Регистрация Blueprint
app.register_blueprint(kafka_blueprint, url_prefix='/api/v1/kafka')

# Используем settings в вашем приложении Flask
@app.route('/')
def index():
    return {
        "KAFKA_TOPIC": settings.KAFKA_TOPIC,
        "KAFKA_HOST": settings.KAFKA_HOST,
        "KAFKA_PORT": settings.KAFKA_PORT,
        "GROUP_ID": settings.GROUP_ID,
        "CONSUMER_TIMEOUT_MS": settings.CONSUMER_TIMEOUT_MS,
        "MAX_RECORDS_PER_CONSUMER": settings.MAX_RECORDS_PER_CONSUMER,
    }

# Регистрация Blueprint
app.register_blueprint(ugc_blueprint, url_prefix='/ugc')

if __name__ == '__main__':
    app.run()
