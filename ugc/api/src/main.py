from flask import Flask
from kafka3 import KafkaProducer
from kafka3.admin import KafkaAdminClient, NewTopic
from kafka3.errors import KafkaConnectionError
from backoff import on_exception, expo

from api.v1.kafka_producer import ugc_blueprint
from config import settings, logger

# TODO: openapi docs
# TODO: синхронизацию с auth-сервисом, только авторизованные пользователи создают события
# TODO: добавить healthcheck  в доккер компоуз к ugc

app = Flask(__name__)

app.register_blueprint(ugc_blueprint)


# TODO: backoff проверить, количество попыток вынести в env\конфиг
# TODO: партицирование  и репликацию доделать, вынести в env\конфиг
@on_exception(expo, (KafkaConnectionError), max_tries=5)
def init_app():
    admin_client = KafkaAdminClient(
        bootstrap_servers=[f'{settings.KAFKA_HOST}:{settings.KAFKA_PORT}'],
        client_id='ugc'
    )
    server_topics = admin_client.list_topics()
    if not settings.KAFKA_TOPIC in server_topics:
        new_topic_list = []
        new_topic_list.append(NewTopic(name=settings.KAFKA_TOPIC, num_partitions=1, replication_factor=1))
        admin_client.create_topics(new_topics=new_topic_list, validate_only=False)

    admin_client.close()


@app.route('/')
def index():
    return "App start"

if __name__ == '__main__':
    init_app()
    # TODO:  вынести в env
    app.run(debug=True)
