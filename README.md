# Проектная работа 8 спринта

Ссылка на репозиторий - [ugc_sprint_1] (https://github.com/Andrei-Mihailov/ugc_sprint_1.git)

# Описание основных сервисов

auth_service - отвечает за аутентификацию пользователей. Стек: FastApi, Redis, Postgres, Jaeger
admin_service - отвечает за наполнение данными базы фильмов. Стек: Django, Postgres
content_service - отвечает за работу с пользовательскими запросами. Стек: FastApi, Redis, Postgres, Elastic
ugc - отвечает за сбор данных об активности пользователей. Стек: Flask, Kafka, Clickhouse

etl (genres, films, persons) - осуществляет перенос данных из admin_service в content_service

## Доступ в панель администратора

Необходимо создать суперпользователя консольной командой в auth-service. Под ним осуществляем вход

```
python src/main.py --email=email --password=password
```

## Доступ к контект сервису

Производим аутентификацию в auth-service, копируем из cookies access_token, добавляем его в авторизацию

```
/auth/api/v1/users/login
```

## Работа с сервисами через docker

http://127.0.0.1/auth/api/openapi
http://127.0.0.1/movies/api/openapi
http://127.0.0.1/admin/
http://127.0.0.1/ugc/send-to-broker/

## Регистрация и аутентификация через Яндекс

Переход по ссылке

```
http://127.0.0.1/auth/api/v1/oauth/yandex/authorize-url
```

перенаправит на страницу авторизации яндекса, после чего полученный код необходимо передать в ручку:

```
/auth/api/v1/oauth/webhook
```

## Партицирование

Выполнено партицирование таблицы authentication по дате авторизации в разрезе месяцев.
Реализация в миграции alembic:
2024_05_05_0529-65a4e8f17754_add_partition_to_auth
Автоматическое создание партиций с использованием pg_partman

## Работа с ugc сервисом
Все события записываются в один топик Kafka, который наполняется любыми видами событий и данными с помощью ручки:
```
/ugc/send-to-broker/movie_events?movie_id=1&user_id=12 - пример
```
Топик Kafka разбирает etl-сервис пачками в соответсвии с настройками в mapping.py.
