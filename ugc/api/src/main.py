from flask import Flask

from api.v1.kafka_producer import ugc_blueprint

# TODO: openapi docs
# TODO: синхронизацию с auth-сервисом, только авторизованные пользователи создают события
# TODO: добавить healthcheck  в доккер компоуз к ugc

app = Flask(__name__)

app.register_blueprint(ugc_blueprint)

@app.route('/')
def index():
    return "App start"

if __name__ == '__main__':
    # TODO:  вынести в env
    app.run(debug=True)
