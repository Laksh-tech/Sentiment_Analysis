import os
import sys
import json
from urllib.parse import urlparse, urlunparse
from celery import Celery
import redis as redis_sync
from sqlalchemy import create_engine, select, func, case, cast, Numeric
from sqlalchemy.orm import sessionmaker

# FORCE SYSTEM PATH IN WORKER ENGINE
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# Clean, beautiful absolute import
from database import Sentiment

RAW_REDIS_URL = os.getenv(
    "REDIS_URL", 
    "rediss://default:gQAAAAAAAbooAAIgcDIxMjA0NTljZWU0ZTU0NjY3YmIwMzY2ZmEyN2Y4ZTRiMw@smiling-bluebird-113192.upstash.io:6379"
)

CELERY_REDIS_URL = f"{RAW_REDIS_URL}?ssl_cert_reqs=none" if RAW_REDIS_URL.startswith("rediss://") and "ssl_cert_reqs" not in RAW_REDIS_URL else RAW_REDIS_URL
    
celery_backend = Celery("tasks", broker=CELERY_REDIS_URL, backend=CELERY_REDIS_URL)

celery_backend.conf.update(
    worker_pool="solo",
    worker_concurrency=1,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
)

redis_connect_args = {"ssl_cert_reqs": None} if RAW_REDIS_URL.startswith("rediss://") else {}
redis_client = redis_sync.from_url(RAW_REDIS_URL, decode_responses=True, **redis_connect_args)

# ── POSTGRES REALIGNMENT ──────────────────────────────────────────────────────
SYNC_DB_URL = os.getenv(
    "SYNC_DATABASE_URL",
    "postgresql+psycopg2://neondb_owner:npg_c8T3HDCUuibR@ep-spring-band-aomqd54n-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"
)

sync_engine = create_engine(
    SYNC_DB_URL, 
    pool_pre_ping=True,
    pool_recycle=1800
)

SessionLocal = sessionmaker(bind=sync_engine, autoflush=False, autocommit=False)

# ── Background Worker Tasks ───────────────────────────────────────────────────
@celery_backend.task
def compute_global_stats():
    """Background task that runs heavy aggregation and warms the Redis cache"""
    with SessionLocal() as session:
        # Standardized math metrics casting to guarantee cross-platform data alignment
        stats_query = session.execute(
            select(
                func.count(Sentiment.id).label('total'),
                # FIXED: Case-insensitive match using func.lower()
                func.sum(case((func.lower(Sentiment.label) == 'positive', 1), else_=0)).label('pos'),
                func.sum(case((func.lower(Sentiment.label) == 'negative', 1), else_=0)).label('neg'),
                func.sum(case((func.lower(Sentiment.label) == 'neutral', 1), else_=0)).label('neu'),
                (cast(func.sum(case((func.lower(Sentiment.label) == 'positive', 1), else_=0)), Float) * 100.0 / 
                 func.nullif(cast(func.sum(case((func.lower(Sentiment.label).in_(['positive', 'negative']), 1), else_=0)), Float), 0.0)
                ).label('pos_rate')
            )
        ).first()

        # Check what values are actually pulled from Postgres in your terminal logs
        print(f"📊 Live Aggregation Sync Metrics -> Total: {stats_query.total}, Pos: {stats_query.pos}, Neg: {stats_query.neg}, Rate: {stats_query.pos_rate}")

        if stats_query and stats_query.total > 0:
            data = {
                "Review_Count": int(stats_query.total),
                "Positive_Reviews": int(stats_query.pos or 0),
                "Negative_Reviews": int(stats_query.neg or 0),
                "Neutral_Reviews": int(stats_query.neu or 0),
                "Overall_Positivity_Rate": float(stats_query.pos_rate) if stats_query.pos_rate else 0.0,
            }
            # FIXED: Added 'ex=600' parameter to match your functional tasks and prevent permanent stale cache locks
            redis_client.set("sentiment_stats", json.dumps(data), ex=600)
            return "Global stats cache warmed successfully"
            
        return "Calculation bypassed: Database sentiments table holds zero records."

@celery_backend.task
def compute_distribution():
    """Warms the distribution + review length cache with explicit Postgres casting."""
    with SessionLocal() as session:
        correlation_rows = session.execute(
            select(
                func.round(cast(Sentiment.score, Numeric), 1).label('rounded_score'),
                Sentiment.label,
                func.count(Sentiment.id).label('count')
            ).group_by(func.round(cast(Sentiment.score, Numeric), 1), Sentiment.label)
        ).all()

        metrics_rows = session.execute(
            select(
                Sentiment.label,
                func.avg(Sentiment.score).label('avg_score'),
                func.count(Sentiment.id).label('count'),
                func.avg(func.length(Sentiment.text)).label('avg_char_count'),
                func.avg(func.length(Sentiment.text) - func.length(func.replace(Sentiment.text, ' ', '')) + 1).label('avg_word_count')
            ).group_by(Sentiment.label)
        ).all()

        # FIXED: Added None checks to every iterable loop block to handle new/empty tracking setups cleanly
        data = {
            "Score_Correlation": [
                {
                    "score": float(r.rounded_score) if r.rounded_score is not None else 0.0, 
                    "label": str(r.label) if r.label else "unknown", 
                    "count": int(r.count or 0)
                }
                for r in correlation_rows
            ],
            "Average_Scores": [
                {
                    "label": str(r.label) if r.label else "unknown", 
                    "average_score": float(r.avg_score or 0.0), 
                    "count": int(r.count or 0)
                }
                for r in metrics_rows
            ],
            "Review_Length_Analysis": [
                {
                    "label":          str(r.label) if r.label else "unknown",
                    "avg_char_count": float(r.avg_char_count or 0.0),
                    "avg_word_count": float(r.avg_word_count or 0.0),
                }
                for r in metrics_rows
            ],
        }
        redis_client.set("sentiment_distribution", json.dumps(data), ex=600)
        return "Distribution cache warmed successfully"

@celery_backend.task
def compute_urgent_reviews():
    with SessionLocal() as session:
        rows = session.execute(
            select(Sentiment.text, Sentiment.score)
            .filter(Sentiment.label == 'negative')
            .order_by(Sentiment.score.asc())
            .limit(10)
        ).all()

        # FIXED: Added string evaluation safety around text captures
        data = {
            "Urgent_Reviews": [
                {"text": str(r.text), "score": float(r.score or 0.0)}
                for r in rows
            ]
        }
        redis_client.set("sentiment_urgent", json.dumps(data), ex=60)
        return "Urgent reviews cache warmed successfully"
