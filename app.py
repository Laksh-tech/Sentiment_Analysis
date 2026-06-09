import asyncio
import json
from datetime import datetime
from fastapi import FastAPI, Depends, HTTPException, APIRouter, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import case, select, insert, func
from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as redis

from database import Base, engine, get_db, Sentiment, ReviewBatch
from sentiment import query_sentiment
from celery_app import compute_global_stats, compute_distribution, compute_urgent_reviews
import os
from dotenv import load_dotenv
load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
# ── Redis client ──────────────────────────────────────────────────────────────
redis_client = redis.from_url(REDIS_URL, decode_responses=True)
# Cache TTLs in seconds — single source of truth
CACHE_TTL = {
    "stats":        300,   # 5 min  — KPI metrics
    "distribution": 600,   # 10 min — chart data (expensive query, changes slowly)
    "urgent":        60,   # 1 min  — critical reviews (time-sensitive)
}

CACHE_KEYS = {
    "stats":        "sentiment_stats",
    "distribution": "sentiment_distribution",
    "urgent":       "sentiment_urgent",
}

app = FastAPI(title="Sentiment Analysis API")
origins = [
    "http://localhost:5173", 
    "http://127.0.0.1:5173","http://localhost:4173/",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def init_tables():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    # Warm all caches on startup so the very first user never hits a cold DB
    _trigger_all_cache_warming()

def _trigger_all_cache_warming():
    """Fire all Celery warming tasks. Non-blocking — workers handle it."""
    compute_global_stats.delay()
    compute_distribution.delay()
    compute_urgent_reviews.delay()

# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

# ── Single review analysis ────────────────────────────────────────────────────
@app.get("/analyse", status_code=201)
async def get_analytics(text: str, session: AsyncSession = Depends(get_db)):
    try:
        result = query_sentiment(text)
        label, score = (
            await result if asyncio.iscoroutine(result) else result
        )
        if label is None:
            raise HTTPException(
                status_code=404,
                detail="Text too short or invalid — no context"
            )
        new_sentiment = Sentiment(
            text=text,
            label=label,
            score=score,
            created_at=datetime.utcnow()
        )
        session.add(new_sentiment)
        await session.flush()
        await session.commit()
        await session.refresh(new_sentiment)

        # Single write — just invalidate stats so next cache read picks it up.
        # Don't block the response on this.
        await redis_client.delete(CACHE_KEYS["stats"])

        return {"text": text, "label": label, "score": score}

    except HTTPException:
        raise
    except Exception as e:
        await session.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ── Batch analysis ────────────────────────────────────────────────────────────
@app.post("/analyse_batch")
async def analyse_batch(batch: ReviewBatch, session: AsyncSession = Depends(get_db)):
    try:
        db_insert_list = []
        results = []

        tasks = [query_sentiment(item.text) for item in batch.reviews]
        all_results = await asyncio.gather(*tasks)

        for i, (label, score) in enumerate(all_results):
            if label:
                def parse_date(date_str: str) -> datetime:
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                        try:
                            return datetime.strptime(date_str, fmt)
                        except ValueError:
                            continue
                    raise ValueError(f"Unrecognised date format: {date_str}")
# then inside analyse_batch, replace datetime.strptime(...) with:
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

            # ── Key change ──────────────────────────────────────────────────
            # Old approach: delete cache keys → first user after ingest hits
            #               cold DB, waits 10s.
            # New approach: fire Celery tasks → workers recompute and write
            #               warm data into Redis before any user requests it.
            _trigger_all_cache_warming()
            # ───────────────────────────────────────────────────────────────

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


# ── Analytics router ──────────────────────────────────────────────────────────
router = APIRouter(prefix="/analytics")

def _cache_miss_response(cache_key: str) -> JSONResponse:
    """
    Returned when cache is cold on first boot and Celery hasn't warmed it yet.
    Tells the frontend to retry in ~3 seconds instead of making the user wait.
    Celery is already computing in the background at this point.
    """
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


# 1. KPI stats — served entirely from Redis
@router.get("/stats")
async def get_analytics_stats():
    cached = await redis_client.get(CACHE_KEYS["stats"])
    if cached:
        return json.loads(cached)

    # Cache cold (e.g. Redis restarted mid-session) — re-trigger warming
    compute_global_stats.delay()
    return _cache_miss_response(CACHE_KEYS["stats"])


# 2. Distribution — served from Redis, supports limit to cut response size
@router.get("/distribution")
async def get_analytics_distribution(
    limit: int = Query(default=100, le=500, description="Max score correlation rows returned")
):
    cached = await redis_client.get(CACHE_KEYS["distribution"])
    if cached:
        data = json.loads(cached)
        # Trim the heaviest key client-side so the full dataset stays cached
        # but individual responses are smaller
        data["Score_Correlation"] = data["Score_Correlation"][:limit]
        return data

    compute_distribution.delay()
    return _cache_miss_response(CACHE_KEYS["distribution"])


# 3. Urgent reviews — served from Redis
@router.get("/reviews/urgent")
async def get_urgent_reviews():
    cached = await redis_client.get(CACHE_KEYS["urgent"])
    if cached:
        return json.loads(cached)

    compute_urgent_reviews.delay()
    return _cache_miss_response(CACHE_KEYS["urgent"])


app.include_router(router)
