# security.py
from flask import request
from functools import wraps
from jwt_utils import parse_token, check_user
from werkzeug.exceptions import Unauthorized, Forbidden


def jwt_required(check_user_flag=False):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth_header = request.headers.get('Authorization', None)
            if not auth_header:
                raise Unauthorized(description="Invalid authorization code.")
            if not auth_header.startswith("Bearer "):
                raise Unauthorized(description="Only Bearer token might be accepted")

            token = auth_header.split(" ")[1]
            decoded_token = parse_token(token)
            if not decoded_token:
                raise Forbidden(description="Invalid or expired token.")

            if check_user_flag:
                check_user({"Authorization": auth_header})

            return f(decoded_token, *args, **kwargs)

        return decorated_function

    return decorator
