poetry run alembic init alembic
poetry run alembic upgrade head
poetry run uvicorn portfolio.app:asgi_app --host "0.0.0.0" --port "5000" --workers 4 --reload
