import json
from datetime import datetime
from http import HTTPStatus

from flask import Blueprint, request, jsonify
from service.kafka_setter import process_get_messages, process_load_kafka

from config import settings, logger

ugc_blueprint = Blueprint('ugc', __name__, url_prefix='/ugc')

# TODO: добавить проверку на структуру данных для отправки в кафку.
# Например, обязязательный параметр тип события и все остальные данные, что пришли в нагрузку
# Разобраться с key, value и headers в кафке, для корректности "складывания" данных
# путь до ручки :5000/ugc/send-to-broker?type_event=click
@ugc_blueprint.route('/send-to-broker',
                     methods=['GET','POST']
)
def send_message_to_kafka():
    data = request.args.to_dict()

    if not data:
        return jsonify({"error": "Data is required"}), HTTPStatus.BAD_REQUEST

    data['current_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        process_load_kafka(json.dumps(data).encode())
        return jsonify({"status": "Message sent to Kafka"}), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR

# для теста получения сообщений через api
# не проверяла
@ugc_blueprint.route('/ugc-consumer', methods=['GET'])
def get_messages_from_kafka():
    try:
        messages = process_get_messages()
        return jsonify([message.dict() for message in messages]), HTTPStatus.OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTPStatus.INTERNAL_SERVER_ERROR
