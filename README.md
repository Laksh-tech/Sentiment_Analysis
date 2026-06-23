# SentimentOps
### Distributed Sentiment Analytics Engine

> **Live API →** [sentiment-analysis-b5el.onrender.com/docs](https://sentiment-analysis-b5el.onrender.com/docs)
> **Live Dashboard →** [sentimentops.lovable.app](https://sentimentops.lovable.app)
> **Dataset:** 19,000+ Zomato & Blinkit reviews · **Ingestion throughput:** ~80 reviews/sec

---

## What This Is

SentimentOps is an asynchronous distributed system that classifies customer reviews using the HuggingFace Inference API, stores results in a serverless PostgreSQL cluster, and serves pre-aggregated analytics to a React dashboard without blocking the API thread.

This project went through two real deployment phases. The first phase ran all three services (API, Celery worker, Celery Beat) continuously on Railway and surfaced several distinct production bugs, documented in full below. The second phase restructured the deployment around free-tier hosting constraints — the honest tradeoffs of that move are explained in the **Current Deployment Model** section, because understanding *why* an architecture looks the way it does matters as much as the architecture itself.

---

## Current Deployment Model

This is not a permanently-running 3-service cluster. Here is exactly how it works right now:

| Component | Where it runs | Behavior |
|---|---|---|
| **FastAPI (Sentiment-Web)** | Render, free Web Service tier | Always reachable. Kept warm by a scheduled GitHub Actions job pinging `/health` every 10 minutes, since free Render web services sleep after inactivity. |
| **Celery Worker** | Run on-demand, locally | Render's free tier does not support Background Workers, and Railway's free worker hours expired. The worker is started manually when fresh data needs to be processed or the cache needs to be refreshed. |
| **Celery Beat** | Not continuously running | Same constraint as the worker. Scheduled recomputation is currently triggered manually rather than via a live 24/7 cron daemon. |
| **Upstash Redis** | Always live (managed cloud service) | Holds the last cached analytics payload indefinitely until manually refreshed. |
| **Neon PostgreSQL** | Always live (managed cloud service) | Holds all 19,000+ rows permanently, independent of any compute host's uptime. |
| **Frontend (Lovable)** | Always live | Displays whatever is currently cached in Redis — effectively a **snapshot** of the last computed analytics run, not a continuously live-updating feed. |

**Why this matters, stated plainly:** the API and dashboard are reachable at any time and will show real, previously computed data. What is *not* always running is the background compute layer (worker + Beat) — because Render's free tier doesn't offer Background Workers and Railway's free worker allowance ran out. Running all three services continuously would cost a small monthly amount; for a portfolio project at this stage, that tradeoff wasn't made yet. The system can be brought fully live on demand by starting the worker locally and pointing it at the same Upstash Redis and Neon Postgres instances the deployed API already uses — they're the same external services either way, so a local worker run updates the exact same cache the live dashboard reads from.

---

## Architecture (Full 3-Service Design, as Originally Deployed)

```
  [Client / React Dashboard]
           │
           ▼
  Sentiment-Web (FastAPI)          ← Serves REST endpoints, issues 202 Accepted immediately
           │
           ▼
  Upstash Redis                    ← Dual role: Celery message broker + analytics cache
      │           │
      ▼           ▼
  Sentiment-Worker           Sentiment-Beat
  (Celery Execution Node)    (Celery Cron Scheduler)
      │                           │
      ▼                           ▼
  Neon PostgreSQL            Broadcasts compute_global_stats,
  (Serverless Cluster)       compute_distribution,
  19,000+ rows               compute_urgent_reviews
```

| Service | Role |
|---|---|
| `Sentiment-Web` | FastAPI server — thin ingest, schema validation, async routing |
| `Sentiment-Worker` | Celery worker (`-P solo`) — heavy computation, DB reads/writes |
| `Sentiment-Beat` | Celery Beat — periodic scheduler broadcasting background tasks |

---

## Tech Stack

| Layer | Technology |
|---|---|
| API Framework | FastAPI (async, non-blocking) |
| Task Queue | Celery + Celery Beat |
| Broker & Cache | Upstash Redis (Edge) |
| Database | Neon PostgreSQL (Serverless) |
| ORM | SQLAlchemy 2.0 (async) + asyncpg |
| ML Inference | HuggingFace Inference API |
| Text Preprocessing | Regex normalization (URLs, emojis, noise) |
| Ingestion | Pandas chunked batch processing |
| Containerization | Docker |
| Cloud Deployment | Render (Web) + local/on-demand (Worker, Beat) |
| Frontend | React via Lovable |

---

## Performance

| Metric | Value |
|---|---|
| Ingestion throughput | ~80 reviews/sec |
| Total records processed | 19,000+ |
| API response model | 202 Accepted (non-blocking) on cache miss |
| Worker isolation | Single-thread (`-P solo`) to avoid async loop collisions |
| Cache invalidation strategy | Event-driven — recompute triggers only after a batch finishes ingesting, not on a blind timer (see Key Architectural Decisions) |

---

## Engineering Log: Production Bugs Solved

### Bug 1 — Async Event Loop Startup Deadlock

**Symptom:** FastAPI froze on deployment, hanging at `Waiting for application startup`. Container was killed by the platform's health checker timeout before binding to its port.

**Root Cause:** The `@app.on_event("startup")` hook ran a schema verification call, `await conn.run_sync(Base.metadata.create_all)`, on every container boot. This DDL-style operation interacted poorly with the connection setup during startup, delaying socket binding past the health check timeout.

**Fix:** Schema creation was moved to a dedicated one-time migration script (`force_neon_init.py`), run manually rather than on every container start. The startup hook now only logs a ready message — no database call at all.

---

### Bug 2 — Serverless Connection Pool Deadlocks via PgBouncer

**Symptom:** Database query channels silently locked mid-session during data processing, with no standard DB exceptions thrown.

**Root Cause:** Neon routes connections through PgBouncer in transaction pooling mode. The `asyncpg` driver generates server-side prepared statements by default. In transaction pooling, sequential operations within a session can be distributed across different backend clusters — so when the driver referenced a prepared statement on an instance that didn't generate it, the session deadlocked silently.

**Fix:** Disabled asyncpg's prepared statement cache in `database.py`:
```python
engine = create_async_engine(
    DATABASE_URL,
    prepared_statement_name_cache_size=0
)
```

---

### Bug 3 — Cross-OS Cache Corruption (WinError 3)

**Symptom:** Celery Beat crashed on the cloud container with `WinError 3: The system cannot find the path specified`.

**Root Cause:** Celery Beat generates local state tracker binaries (`celerybeat-schedule.dat`, `.bak`) during development. On a Windows dev machine, these embedded absolute Windows paths. The files were accidentally committed to Git, and the Linux container choked reading Windows-style paths.

**Fix:** Removed the binaries from the Git index and excluded them going forward:
```
sentimentops-backend/celerybeat-schedule*
pipeline.log
__pycache__/
```

---

### Bug 4 — Container Namespace Collision (ModuleNotFoundError)

**Symptom:** The web container booted correctly. The worker container crashed immediately with `ModuleNotFoundError: No module named 'database'`.

**Root Cause:** The codebase lives inside a subfolder (`sentimentops-backend/`). The Dockerfile set `WORKDIR /app`. When Celery spawned its internal process, it resolved module lookups from `/app`, losing visibility of sibling modules like `database.py` inside the subfolder.

**Fix:** Set `WORKDIR` directly to the subfolder and bound `PYTHONPATH` to the same path inside the Dockerfile itself, so the setting survives regardless of which process (Uvicorn, Celery worker, or Beat) starts the container:
```dockerfile
ENV PYTHONPATH=/app/sentimentops-backend
WORKDIR /app/sentimentops-backend
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000}"]
```

---

### Bug 5 — CORS Preflight Rejection (Blank Frontend)

**Symptom:** Backend logs showed clean operations, but the React frontend was completely blank with a browser console error: `Response to preflight request doesn't pass access control check`.

**Root Cause:** Lovable generates dynamic sandbox subdomains (`*.lovableproject.com`) for frontend previews. The backend's strict CORS policy rejected the dynamic origin, blocking all client-side data fetches.

**Fix:** Updated CORS middleware to allow all origins, acceptable here since the backend serves only aggregate, read-only analytics with no user-specific or write-sensitive data exposed to the frontend:
```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
```

---

### Bug 6 — Hardcoded Credentials in Source Code

**Symptom:** Database and Redis connection strings, including live passwords, were committed directly into `database.py`, `celery_app.py`, and `force_neon_init.py` as default fallback values inside `os.getenv("VAR", "real-credential-here")` calls — exposing them in the public GitHub repository.

**Root Cause:** Using a literal connection string as the second argument to `os.getenv()` means that string is the value used whenever the environment variable isn't set — and it's also permanently visible in source control regardless of whether the env var is set correctly in production.

**Fix:** Removed all hardcoded fallback values. Every credential now fails loudly at startup if missing, instead of silently falling back to an exposed value:
```python
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable not set")
```
All exposed credentials (Upstash Redis password, Neon Postgres password) were rotated after removal from source.

---

### Bug 7 — Cache Invalidation Storm Under Bulk Ingestion

**Symptom:** Upstash Redis usage was exhausted well before any heavy traffic — far faster than the request volume seemed to justify.

**Root Cause:** The single-review ingestion endpoint called `redis_client.delete(CACHE_KEYS["stats"])` on every insert. During bulk ingestion of 19,000+ rows at ~80 inserts/sec, this meant the stats cache was invalidated roughly 80 times per second, and every subsequent read became a cache miss, each one separately enqueueing a new Celery recompute task — compounding the load far beyond what the actual read traffic required.

**Fix:** Removed per-row cache invalidation from the single-review endpoint. Cache recomputation is now triggered exactly once, after a full batch finishes inserting — not per row, and not on a fixed timer:
```python
if db_insert_list:
    await session.execute(insert(Sentiment), db_insert_list)
    await session.commit()
    compute_global_stats.delay()
    compute_distribution.delay()
    compute_urgent_reviews.delay()
```
A Redis-backed lock (`SET ... NX EX 10`) was also added on each analytics read route to prevent duplicate recompute tasks from stacking up if multiple dashboard reads land while a computation is already in flight — a standard cache-stampede prevention pattern.

---

## Project Structure

```
Sentiment_Analysis/
├── sentimentops-backend/
│   ├── app.py              # FastAPI routing and lifecycle
│   ├── database.py         # Async engine, session management
│   ├── models.py           # SQLAlchemy 2.0 schema (DeclarativeBase)
│   ├── celery_app.py       # Celery worker task definitions
│   └── force_neon_init.py  # One-time schema migration utility
├── Dockerfile
├── requirements.txt
└── .env.production         # Local-only, never committed
```

---

## Key Architectural Decisions

**Why Celery + Beat instead of FastAPI BackgroundTasks?**
FastAPI's built-in background tasks share the event loop with the API server. For 19K+ row processing, this would degrade API response times. Celery isolates compute entirely — the API issues a `202 Accepted` instantly and the worker handles the rest independently.

**Why `-P solo` for the worker?**
Celery's default multiprocessing prefork model conflicts with async code. Solo mode runs a single-threaded execution model inside the worker, avoiding async context conflicts when writing through the async SQLAlchemy session used elsewhere in the codebase.

**Why event-driven cache recomputation instead of a fixed timer?**
The original design used Celery Beat to blindly recompute analytics on a fixed interval regardless of whether new data had arrived. This wasted compute and Redis requests during idle periods, and was the root cause of Bug 7's invalidation storm. Recomputation is now triggered by the event that actually changes the underlying data — the end of a batch insert — rather than by a clock.

**Why dual-role Redis?**
Using Upstash Redis as both the Celery message broker and the analytics cache eliminates a second managed service and keeps the architecture's moving parts to a minimum.

---

*Built by [Lakshya Singh Kushwah](https://github.com/Laksh-tech)*
