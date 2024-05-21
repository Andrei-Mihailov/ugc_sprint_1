from pydantic import BaseModel
import json

# Создание функции сериализации, совместимой с Flask
def flask_json_dumps(v):
    return json.dumps(v)

class Event(BaseModel):
    movie_timestamp: int
    movie_id: str
    user_id: int

    class Config:
        # Использование стандартной функции сериализации для Flask
        json_dumps = flask_json_dumps

class UserValues(BaseModel):
    value: str

    class Config:
        # Использование стандартной функции сериализации для Flask
        json_dumps = flask_json_dumps