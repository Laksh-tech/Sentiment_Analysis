web: cd sentimentops-backend && uvicorn app:app --host 0.0.0.0 --port $PORT
worker: cd sentimentops-backend && celery -A celery_app worker --loglevel=info -P solo
beat: cd sentimentops-backend && celery -A celery_app beat --loglevel=info