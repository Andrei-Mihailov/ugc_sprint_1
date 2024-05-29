from flask import Flask, jsonify, request
from kafka3.admin import KafkaAdminClient, NewTopic
from kafka3.errors import KafkaConnectionError
from backoff import on_exception, expo
from flask_jwt_extended import (
    JWTManager,
    create_access_token,
    jwt_required,
    get_jwt_identity,
)

from api.v1.kafka_producer import ugc_blueprint
from config import settings
from dotenv import load_dotenv
import os

load_dotenv()

# TODO: openapi docs - думаю нашей схемы в качестве документации будет достаточно, если нет, то сделаю
# TODO: синхронизацию с auth-сервисом, только авторизованные пользователи создают события +- (в процессе)
# TODO: добавить healthcheck  в доккер компоуз к ugc+

app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY", "your_secret_key")
jwt = JWTManager(app)

app.register_blueprint(ugc_blueprint)


# TODO: backoff проверить, количество попыток вынести в env\конфиг+
# TODO: партицирование  и репликацию доделать, вынести в env\конфиг+
@on_exception(expo, (KafkaConnectionError), max_tries=int(os.getenv("MAX_TRIES")))
def init_app():
    admin_client = KafkaAdminClient(
        bootstrap_servers=[f"{settings.KAFKA_HOST}:{settings.KAFKA_PORT}"],
        client_id="ugc",
    )
    server_topics = admin_client.list_topics()
    if settings.KAFKA_TOPIC not in server_topics:
        new_topic_list = [
            NewTopic(
                name=settings.KAFKA_TOPIC,
                num_partitions=settings.NUM_PARTITIONS,
                replication_factor=settings.REPLICATION_FACTOR,
            )
        ]
        admin_client.create_topics(new_topics=new_topic_list, validate_only=False)

    admin_client.close()


@app.route("/")
def index():
    return "App start"


@app.route("/login", methods=["POST"])
def login():
    username = request.json.get("username", None)
    password = request.json.get("password", None)
    # TODO admin admin не годится
    if username != "admin" or password != "admin":
        return jsonify({"msg": "Bad username or password"}), 401

    access_token = create_access_token(identity=username)
    return jsonify(access_token=access_token), 200


@app.route("/create_event", methods=["POST"])
@jwt_required()
def create_event():
    user_id = get_jwt_identity()
    return jsonify(message="Event created by user: " + user_id), 200


if __name__ == "__main__":
    init_app()
    # TODO:  вынести в env+
    app.run(debug=bool(os.getenv("DEBUG")))
