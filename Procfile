web: uvicorn sentimentops-backend.app:app --host 0.0.0.0 --port $PORT
worker: celery -A sentimentops-backend.celery_app worker --loglevel=info -P solo
beat: celery -A sentimentops-backend.celery_app beat --loglevel=info