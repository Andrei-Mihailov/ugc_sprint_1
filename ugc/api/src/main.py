from flask import Flask
from kafka3.admin import KafkaAdminClient, NewTopic
from kafka3.errors import KafkaConnectionError
from backoff import on_exception, expo
from flask_jwt_extended import JWTManager

from api.v1.kafka_producer import ugc_blueprint
from config import settings


app = Flask(__name__)
app.config["JWT_SECRET_KEY"] = settings.JWT_SECRET_KEY
jwt = JWTManager(app)

app.register_blueprint(ugc_blueprint)


@on_exception(expo, (KafkaConnectionError), max_tries=settings.MAX_TRIES)
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


if __name__ == "__main__":
    init_app()
    app.run(debug=settings.DEBUG)
