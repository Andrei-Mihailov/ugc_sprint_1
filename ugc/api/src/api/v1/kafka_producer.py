from flask import Blueprint, request, jsonify
import json
from datetime import datetime
from typing import List
from service.kafka_setter import process_get_messages, process_load_kafka
from models.models import Event, UserValues

# Создаем Blueprint для маршрутов связанных с UGC
ugc_blueprint = Blueprint('ugc', __name__)


@ugc_blueprint.route('/ugc-producer', methods=['POST'])
def kafka_load():
    """
    Производит тестовые UGC сообщения в Kafka.
    """
    # Получаем данные из запроса
    data = request.json
    if not data:
        return jsonify({"error": "Data is required"}), 400

    # Преобразуем данные в объект Event
    event = Event(**data)
    event_dict = event.dict()

    # Добавляем текущую дату и время
    event_dict['date_time'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    my_info = json.dumps(event_dict).encode()

    try:
        # Отправляем данные в Kafka
        result = process_load_kafka(value=my_info)
        return '', 204
    except Exception as e:
        # В случае ошибки отправляем HTTP 500 и сообщение об ошибке
        return jsonify({"error": str(e)}), 500


@ugc_blueprint.route('/ugc-consumer', methods=['GET'])
def get_messages_from_kafka():
    """
    Получает список сообщений из Kafka.
    """
    try:
        # Получаем сообщения из Kafka
        messages = process_get_messages()
        # Возвращаем сообщения в виде списка JSON объектов и HTTP 200
        return jsonify([message.dict() for message in messages]), 200
    except Exception as e:
        # В случае ошибки отправляем HTTP 500 и сообщение об ошибке
        return jsonify({"error": str(e)}), 500
