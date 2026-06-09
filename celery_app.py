from celery import Celery
from celery.schedules import crontab
import asyncio
import json
from database import engine, Sentiment
from sqlalchemy import create_engine
from sqlalchemy import select, func, case
import redis as redis_sync # Celery workers typically run synchronously
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# # Initialize Celery
celery_backend = Celery(
    "tasks",
    broker=REDIS_URL,
    backend=REDIS_URL
)
# Also fix the sync redis client inside tasks:
redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)
# And fix the sync DB URL inside each task function:
SYNC_DB_URL = os.getenv(
    "SYNC_DATABASE_URL",
    "mysql+pymysql://root:root@localhost/sentiment_db"
)
# celery_app.py — replace create_engine line inside each task
SYNC_DB_URL = SYNC_DB_URL.split("?")[0]

ssl_args = {"ssl": {"ssl_disabled": False}} if "aivencloud" in SYNC_DB_URL else {}
sync_engine = create_engine(SYNC_DB_URL, connect_args=ssl_args)

celery_backend.conf.update(
    worker_pool="solo",               # no forking on Windows
    worker_concurrency=1,             # solo pool is always 1
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)

@celery_backend.task
def compute_global_stats():
    """Background task that runs heavy aggregation and warms the Redis cache"""
    # Since Celery runs in a separate synchronous worker process, we use a standard connection block
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    
    ssl_args = {"ssl": {"ssl_disabled": False}} if "aivencloud" in SYNC_DB_URL else {}
    sync_engine = create_engine(SYNC_DB_URL, connect_args=ssl_args)

    
    # Creating a temporary sync sync engine just for the background worker metric computation
    Session = sessionmaker(bind=sync_engine)

    with Session() as session:
        # Execute your heavy global stats query
        stats_query = session.execute(
            select(
                func.count(Sentiment.id).label('total'),
                func.count(case((Sentiment.label == 'positive', 1))).label('pos'),
                func.count(case((Sentiment.label == 'negative', 1))).label('neg'),
                func.count(case((Sentiment.label == 'neutral', 1))).label('neu'),
                (func.count(case((Sentiment.label == 'positive', 1))) * 100.0 / 
                 func.nullif(func.count(case((Sentiment.label.in_(['positive', 'negative']), 1))), 0)).label('pos_rate')
            )
        ).first()

        if stats_query:
            data = {
                "Review_Count": int(stats_query.total) if stats_query.total else 0,
                "Positive_Reviews": int(stats_query.pos) if stats_query.pos else 0,
                "Negative_Reviews": int(stats_query.neg) if stats_query.neg else 0,
                "Neutral_Reviews": int(stats_query.neu) if stats_query.neu else 0,
                "Overall_Positivity_Rate": float(stats_query.pos_rate) if stats_query.pos_rate else 0.0,
            }
            # Warm up the cache by writing directly to Redis
            redis_client.set("sentiment_stats", json.dumps(data))
            return "Cache Warmed Successfully"

# Celery Beat Schedule Configuration to automate computation every 10 minutes
celery_backend.conf.beat_schedule = {
    "warm-stats-cache-every-10-mins": {
        "task": "celery_app.compute_global_stats",
        "schedule": 600.0, # 10 minutes in seconds
    },
}

# Add these two tasks to your existing celery_app.py
@celery_backend.task
def compute_distribution():
    """Warms the distribution + review length cache synchronously."""
    from sqlalchemy import create_engine, func, select 
    from sqlalchemy.orm import sessionmaker
    
    ssl_args = {"ssl": {"ssl_disabled": False}} if "aivencloud" in SYNC_DB_URL else {}
    sync_engine = create_engine(SYNC_DB_URL, connect_args=ssl_args)

    # Using standard pymysql for synchronous worker operations
    SessionLocal = sessionmaker(bind=sync_engine, autoflush=False, autocommit=False)

    with SessionLocal() as session:
        # 1. Score Correlation Query
        correlation_rows = session.execute(
            select(
                func.round(Sentiment.score, 1).label('rounded_score'),
                Sentiment.label,
                func.count(Sentiment.id).label('count')
            ).group_by(func.round(Sentiment.score, 1), Sentiment.label)
        ).all()

        # 2. Metrics per Label Query
        metrics_rows = session.execute(
            select(
                Sentiment.label,
                func.avg(Sentiment.score).label('avg_score'),
                func.count(Sentiment.id).label('count'),
                func.avg(func.length(Sentiment.text)).label('avg_char_count'),
                func.avg(
                    func.length(Sentiment.text)
                    - func.length(func.replace(Sentiment.text, ' ', ''))
                    + 1
                ).label('avg_word_count')
            ).group_by(Sentiment.label)
        ).all()

        # 3. Clean Serialization Matching Your Dashboard Format
        data = {
            "Score_Correlation": [
                {"score": float(r.rounded_score), "label": str(r.label), "count": int(r.count)}
                for r in correlation_rows
            ],
            "Average_Scores": [
                {"label": str(r.label), "average_score": float(r.avg_score or 0), "count": int(r.count)}
                for r in metrics_rows
            ],
            "Review_Length_Analysis": [
                {
                    "label":          str(r.label),
                    "avg_char_count": float(r.avg_char_count or 0),
                    "avg_word_count": float(r.avg_word_count or 0),
                }
                for r in metrics_rows
            ],
        }
        
        # Save into the Redis cache for your FastAPI app to grab instantly
        redis_client.set("sentiment_distribution", json.dumps(data), ex=600)
        return "Distribution cache warmed successfully"

@celery_backend.task
def compute_urgent_reviews():
    """Warms the top-10 urgent negative reviews cache."""
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    
    ssl_args = {"ssl": {"ssl_disabled": False}} if "aivencloud" in SYNC_DB_URL else {}
    sync_engine = create_engine(SYNC_DB_URL, connect_args=ssl_args)

    Session = sessionmaker(bind=sync_engine)

    with Session() as session:
        rows = session.execute(
            select(Sentiment.text, Sentiment.score)
            .filter(Sentiment.label == 'negative')
            .order_by(Sentiment.score.asc())
            .limit(10)
        ).all()

        data = {
            "Urgent_Reviews": [
                {"text": r.text, "score": float(r.score)}
                for r in rows
            ]
        }
        redis_client.set("sentiment_urgent", json.dumps(data), ex=60)
        return "Urgent reviews cache warmed"
    
    