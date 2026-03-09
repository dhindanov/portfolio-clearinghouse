FROM python:3.14-slim
RUN apt-get update
RUN apt-get install -y python3-dev default-libmysqlclient-dev build-essential pkg-config

WORKDIR /app
RUN pip install poetry
COPY pyproject.toml ./
RUN poetry config virtualenvs.create true && poetry install --no-interaction --no-root
RUN poetry run alembic init alembic
RUN poetry run alembic upgrade head
