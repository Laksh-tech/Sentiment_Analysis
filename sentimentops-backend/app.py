import asyncio
import json
import os
import sys  # 1. Import sys to manipulate execution tracks
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis
from dotenv import load_dotenv

# 2. FORCE SYSTEM PATH TO INCLUDE SUBFOLDER (Resolves Railway execution handshakes)
# This appends the exact directory where app.py lives into Python's lookup registry
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

# 3. CLEAN STANDARD IMPORTS (Now works flawlessly anywhere!)
from database import Base, engine, get_db, Sentiment, ReviewBatch
from sentiment import query_sentiment
from celery_app import compute_global_stats, compute_distribution, compute_urgent_reviews

load_dotenv()
# ── REDIS CONFIGURATION (Upstash Edge Optimization) ───────────────────────────
RAW_REDIS_URL = os.getenv(
    "REDIS_URL", 
    "rediss://default:gQAAAAAAAbooAAIgcDIxMjA0NTljZWU0ZTU0NjY3YmIwMzY2ZmEyN2Y4ZTRiMw@smiling-bluebird-113192.upstash.io:6379"
)

redis_connect_args = {}
if RAW_REDIS_URL.startswith("rediss://"):
    redis_connect_args["ssl_cert_reqs"] = None  # Prevents edge network handshake timeouts

redis_client = redis.from_url(RAW_REDIS_URL, decode_responses=True, **redis_connect_args)

CACHE_KEYS = {
    "stats":        "sentiment_stats",
    "distribution": "sentiment_distribution",
    "urgent":       "sentiment_urgent",
}

app = FastAPI(title="SentimentOps Production Core API")

# ── DYNAMIC CORS CONFIGURATION ────────────────────────────────────────────────
# Captures your live Lovable, Vercel, or Local domains dynamically from environment variables
allowed_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173"
]

# Read the custom production frontend URL dynamically if set in Railway variables
ENV_FRONTEND = os.getenv("FRONTEND_URL")
if ENV_FRONTEND:
    allowed_origins.append(ENV_FRONTEND)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── LIFECYCLE HOOKS ───────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    # Tables are pre-carved via force_neon_init.py; we can boot instantly without deadlocks!
    print("🚀 SentimentOps Production Engine Online & Connected to Neon Cluster.")

# ── DATE PARSER ───────────────────────────────────────────────────────────────
def parse_date(date_str: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format: {date_str}")

# ── HEALTH CHECK ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# ── SINGLE REVIEW INGESTION ───────────────────────────────────────────────────
@app.get("/analyse", status_code=201)
async def get_analytics(text: str, session: AsyncSession = Depends(get_db)):
    try:
        result = query_sentiment(text)
        label, score = await result if asyncio.iscoroutine(result) else result
        
        if label is None:
            raise HTTPException(status_code=404, detail="Text too short or invalid")
            
        new_sentiment = Sentiment(
            text=text,
            label=label,
            score=score,
            created_at=datetime.utcnow()
        )
        session.add(new_sentiment)
        await session.flush()
        await session.commit()

        await redis_client.delete(CACHE_KEYS["stats"])
        return {"text": text, "label": label, "score": score}
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ── BATCH REVIEW INGESTION ────────────────────────────────────────────────────
@app.post("/analyse_batch")
async def analyse_batch(batch: ReviewBatch, session: AsyncSession = Depends(get_db)):
    try:
        db_insert_list = []
        results = []

        tasks = [query_sentiment(item.text) for item in batch.reviews]
        all_results = await asyncio.gather(*tasks)

        for i, (label, score) in enumerate(all_results):
            if label:
                date_parsed = parse_date(batch.reviews[i].date)
                db_insert_list.append({
                    "text":       batch.reviews[i].text,
                    "label":      label,
                    "score":      score,
                    "created_at": date_parsed,
                })
                results.append({
                    "text":  f"{batch.reviews[i].text[:30]}...",
                    "label": label,
                })
                
        if db_insert_list:
            await session.execute(insert(Sentiment), db_insert_list)
            await session.flush()
            await session.commit()
            
        return {
            "status":    "success",
            "processed": len(db_insert_list),
            "results":   results,
        }
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

# ── ANALYTICS ROUTER ──────────────────────────────────────────────────────────
router = APIRouter(prefix="/analytics")

def _cache_miss_response(cache_key: str) -> JSONResponse:
    return JSONResponse(
        status_code=202,
        content={
            "status":      "warming",
            "message":     "Analytics are being computed. Retry in 3 seconds.",
            "retry_after": 3,
            "cache_key":   cache_key,
        },
        headers={"Retry-After": "3"},
    )

@router.get("/stats")
async def get_analytics_stats():
    cached = await redis_client.get(CACHE_KEYS["stats"])
    if cached:
        return json.loads(cached)
        
    compute_global_stats.delay()
    return _cache_miss_response(CACHE_KEYS["stats"])

@router.get("/distribution")
async def get_analytics_distribution(
    limit: int = Query(default=100, le=500, description="Max score correlation rows returned")
):
    cached = await redis_client.get(CACHE_KEYS["distribution"])
    if cached:
        data = json.loads(cached)
        data["Score_Correlation"] = data["Score_Correlation"][:limit]
        return data
        
    compute_distribution.delay()
    return _cache_miss_response(CACHE_KEYS["distribution"])

@router.get("/reviews/urgent")
async def get_urgent_reviews():
    cached = await redis_client.get(CACHE_KEYS["urgent"])
    if cached:
        return json.loads(cached)
        
    compute_urgent_reviews.delay()
    return _cache_miss_response(CACHE_KEYS["urgent"])

app.include_router(router)