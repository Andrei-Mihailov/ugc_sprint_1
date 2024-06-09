import jwt
import aiohttp
import asyncio

from flask import request
from flask_jwt_extended import jwt_required, decode_token
from werkzeug.exceptions import HTTPException

from src.config import settings


def decode_jwt(jwt_token: str,
               private_key: str = 'your-secret-key',
               algorithm: str = 'HS256'):
    try:
        return decode_token(jwt_token)
    except jwt.exceptions.DecodeError:
        raise HTTPException(description='Invalid authentication credentials', code=401)
    except jwt.exceptions.InvalidAlgorithmError:
        raise HTTPException(description='Invalid token algorithm', code=401)
    except jwt.exceptions.InvalidSignatureError:
        raise HTTPException(description='Invalid token signature', code=401)
    except jwt.exceptions.ExpiredSignatureError:
        raise HTTPException(description='Token has expired, refresh token', code=401)


class JWTBearer:
    def __init__(self, check_user: bool = False, auto_error: bool = True):
        self.check_user = check_user
        self.auto_error = auto_error

    def __call__(self, f):
        @jwt_required()
        def wrapper(*args, **kwargs):
            credentials = request.headers.get('Authorization')
            if not credentials:
                raise HTTPException(description='Invalid authorization code.', code=403)
            if not credentials.startswith('Bearer '):
                raise HTTPException(description='Only Bearer token might be accepted', code=401)
            token = credentials.split()[1]
            decoded_token = self.parse_token(token)
            if not decoded_token:
                raise HTTPException(description='Invalid or expired token.', code=403)

            if self.check_user:
                loop = asyncio.get_event_loop()
                response = loop.run_until_complete(self.check(
                    settings.AUTH_API_ME_URL, headers={'Authorization': f'Bearer {token}'}))
                if response.status != 202:
                    raise HTTPException(description='User doesn\'t exist', code=403)

            return f(*args, **kwargs)
        return wrapper

    def parse_token(self, jwt_token: str) -> dict:
        try:
            return decode_jwt(jwt_token)
        except Exception:
            return {}

    @staticmethod
    async def check(query: str, params: dict = {}, headers: dict = {}, json: dict = {}):
        async with aiohttp.ClientSession(headers=headers) as client:
            response = await client.get(query, json=json, params=params)
            return response


security_jwt = JWTBearer()
security_jwt_check = JWTBearer(check_user=True)
