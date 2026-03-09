poetry run python rundbinit.py
poetry run uvicorn portfolio.app:asgi_app --host "0.0.0.0" --port "5000" --workers 4 --reload
