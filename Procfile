web: uvicorn app:app --host 0.0.0.0 --port $PORT
worker: celery -A celery_app worker --loglevel=info
beat: celery -A celery_app beat --loglevel=info