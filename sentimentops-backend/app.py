import asyncio
import json
import os
import sys  # Imported to manipulate the execution tracks
# ── PATH INJECTION FIX ──
# Dynamically resolves the path so Uvicorn can find your module dependencies
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, APIRouter, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis
# Clean, standard, absolute imports will now map perfectly
from database import Base, engine, get_db, Sentiment, ReviewBatch
from sentiment import query_sentiment
from celery_app import compute_global_stats, compute_distribution, compute_urgent_reviews
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env.production"))

# ── REDIS CONFIGURATION (Upstash Edge Optimization) ───────────────────────────
# 1. Safely retrieve the environment variable
RAW_REDIS_URL = os.getenv("REDIS_URL")
# print("Loaded REDIS_URL:", os.getenv("REDIS_URL"))  # remove after confirming
# 2. Initialize connection arguments
redis_connect_args = {}

# 3. Guard against None and check for secure connection
if RAW_REDIS_URL and RAW_REDIS_URL.startswith("rediss://"):
    # Prevents edge network handshake timeouts by disabling strict certificate checks
    redis_connect_args["ssl_cert_reqs"] = None
# 4. Initialize client only if URL is present
if RAW_REDIS_URL:
    redis_client = redis.from_url(
        RAW_REDIS_URL, 
        decode_responses=True, 
        **redis_connect_args
    )
else:
    # Fallback for local development or missing configurations
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

CACHE_KEYS = {
    "stats":        "sentiment_stats",
    "distribution": "sentiment_distribution",
    "urgent":       "sentiment_urgent",
}

app = FastAPI(title="SentimentOps Production Core API")
# ── RELAXED CORS FOR DEVELOPMENT & DEPLOYMENT ─────────────────────────────────
# Allows seamless cross-origin requests from any Lovable preview or Vercel container
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # FIXED: Open to all domains to bypass browser blocks permanently
    allow_credentials=False,  # Must be False when origin is '*'
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
        
        # Single-review inserts don't trigger recompute — low frequency,
        # Beat's periodic refresh or the next batch run will catch it.
        # If live single-submission freshness becomes a requirement,
        # switch to: compute_global_stats.delay()
        
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
            
            # NEW: trigger recompute once, after the whole batch lands
            compute_global_stats.delay()
            compute_distribution.delay()
            compute_urgent_reviews.delay()
            
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
    lock_acquired = await redis_client.set("lock:stats_compute", "1", nx=True, ex=10)
    if lock_acquired:
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
    lock_acquired = await redis_client.set("lock:distribution_compute", "1", nx=True, ex=10)
    if lock_acquired:   
        compute_distribution.delay()
    return _cache_miss_response(CACHE_KEYS["distribution"])

@router.get("/reviews/urgent")
async def get_urgent_reviews():
    cached = await redis_client.get(CACHE_KEYS["urgent"])
    if cached:
        return json.loads(cached)
    lock_acquired = await redis_client.set("lock:urgent_review_compute", "1", nx=True, ex=10)
    if lock_acquired:  
        compute_urgent_reviews.delay()
    return _cache_miss_response(CACHE_KEYS["urgent"])

app.include_router(router)
