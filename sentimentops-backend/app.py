import asyncio
import json
import os
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis
from dotenv import load_dotenv

# App core dependencies
from database import Base, engine, get_db, Sentiment, ReviewBatch
from sentiment import query_sentiment
from celery_app import compute_global_stats, compute_distribution, compute_urgent_reviews

load_dotenv()

# FORCE INJECT: Hardcode your Upstash link as the default fallback string if environment variables are blank
RAW_REDIS_URL = os.getenv(
    "REDIS_URL", 
    "rediss://default:gQAAAAAAAbooAAIgcDIxMjA0NTljZWU0ZTU0NjY3YmIwMzY2ZmEyN2Y4ZTRiMw@smiling-bluebird-113192.upstash.io:6379"
)

# SDE-1 Edge Case Config: Ensure explicit SSL argument mapping for Secure Redis (rediss://)
redis_connect_args = {}
if RAW_REDIS_URL.startswith("rediss://"):
    redis_connect_args["ssl_cert_reqs"] = None  # Permits handshakes across serverless edge networks

# Initialize the async client with the secure path parameters
redis_client = redis.from_url(RAW_REDIS_URL, decode_responses=True, **redis_connect_args)
# ── Redis Client Initialization (Optimized for Upstash TLS) ────────────────────
# Cache keys single source of truth configurations
CACHE_KEYS = {
    "stats":        "sentiment_stats",
    "distribution": "sentiment_distribution",
    "urgent":       "sentiment_urgent",
}

app = FastAPI(title="Sentiment Analysis API")

# ── CORS Middleware Configuration (Dynamic for Vercel Deployment) ──────────────
# Pull the Vercel domain from variables; fallback to localhost for development
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

origins = [
    FRONTEND_URL,
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ── Lifecycle Hooks ───────────────────────────────────────────────────────────
# ── Clean Production Lifecycle Hooks ───────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    # FIXED: Remove the blocking run_sync metadata call since tables are already built on Neon.
    # This prevents the Uvicorn thread from locking up behind the serverless proxy gateway!
    print("🚀 SentimentOps Production Engine Online & Handshaking with Neon Database Cluster!")
    # COMMENT THIS OUT TEMPORARILY:
    # _trigger_all_cache_warming()
     
def _trigger_all_cache_warming():
    """Fire all Celery warming tasks. Non-blocking — workers handle it."""
    compute_global_stats.delay()
    compute_distribution.delay()
    compute_urgent_reviews.delay()
# ── Helper Date Parser (Moved outside loops to optimize memory) ──────────────
def parse_date(date_str: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognised date format: {date_str}")
# ── Health Check ──────────────────────────────────────────────────────────────
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# ── Single Review Ingestion ───────────────────────────────────────────────────
@app.get("/analyse", status_code=201)
async def get_analytics(text: str, session: AsyncSession = Depends(get_db)):
    try:
        result = query_sentiment(text)
        label, score = await result if asyncio.iscoroutine(result) else result
        
        if label is None:
            raise HTTPException(status_code=404, detail="Text too short or invalid — no context")
            
        new_sentiment = Sentiment(
            text=text,
            label=label,
            score=score,
            created_at=datetime.utcnow()
        )
        session.add(new_sentiment)
        await session.flush()
        await session.commit()

        # Invalidate stats so next cache read picks it up.
        await redis_client.delete(CACHE_KEYS["stats"])
        return {"text": text, "label": label, "score": score}
    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ── Batch Review Ingestion ────────────────────────────────────────────────────
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
            
            # REMOVE OR COMMENT this line out:
            # _trigger_all_cache_warming()  <--- This was flooding your queue 1,000 times!
            
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

# ── Analytics Segmented Router ────────────────────────────────────────────────
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
