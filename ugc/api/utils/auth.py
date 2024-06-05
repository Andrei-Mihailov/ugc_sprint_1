import jwt
from werkzeug.exceptions import Unauthorized, Forbidden
from core.config import settings
import requests

def decode_jwt(
    jwt_token: str,
    private_key: str = settings.auth_jwt.secret_key,
    algorithm: str = settings.auth_jwt.algorithm,
):
    try:
        decoded = jwt.decode(jwt_token, private_key, algorithms=[algorithm])
    except jwt.DecodeError:
        raise Unauthorized(description="Недействительные учетные данные для аутентификации")
    except jwt.InvalidAlgorithmError:
        raise Unauthorized(description="Недействительный алгоритм токена")
    except jwt.InvalidSignatureError:
        raise Unauthorized(description="Недействительная подпись токена")
    except jwt.ExpiredSignatureError:
        raise Unauthorized(description="Срок действия токена истек, обновите токен")
    return decoded

def parse_token(jwt_token: str) -> dict:
    return decode_jwt(jwt_token)

def check_user(headers: dict) -> bool:
    response = requests.get("http://127.0.0.1:8080/api/v1/users/me", headers=headers)
    if response.status_code != 202:
        raise Forbidden(description="Пользователь не существует")
    return True
