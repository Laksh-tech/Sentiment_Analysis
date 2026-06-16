# SentimentOps
### Distributed High-Throughput Sentiment Analytics Engine

> **Live Demo →** [sentimentops.lovable.app](https://sentimentops.lovable.app)  
> **Backend:** Railway Cloud (3 isolated containers) · **Frontend:** Lovable  
> **Dataset:** 19,000+ Zomato & Blinkit reviews · **Ingestion throughput:** ~80 reviews/sec

---

## What This Is

SentimentOps is an asynchronous distributed system that classifies large volumes of customer reviews using the HuggingFace Inference API, stores results in a serverless PostgreSQL cluster, and serves pre-aggregated analytics to a React dashboard — without ever blocking the primary API thread.

This is not a tutorial project. It's a production deployment that broke in real ways and was fixed with real engineering decisions. The full bug log is documented below.

---

## Architecture

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
                             every 180 seconds → keeps cache pre-warmed
```

**Three Railway services running in production:**

| Service | Role |
|---|---|
| `Sentiment-Web` | FastAPI server — thin ingest, schema validation, async routing |
| `Sentiment-Worker` | Celery worker (`-P solo`) — heavy computation, DB reads/writes |
| `Sentiment-Beat` | Celery Beat — 180s cron loop broadcasting background tasks |

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
| Containerization | Docker + Docker Compose |
| Cloud Deployment | Railway (multi-service) |
| Frontend | React via Lovable |

---

## Performance

| Metric | Value |
|---|---|
| Ingestion throughput | ~80 reviews/sec |
| Total records processed | 19,000+ |
| Cache refresh interval | 180 seconds (Beat cron) |
| API response model | 202 Accepted (non-blocking) |
| Worker isolation | Single-thread (`-P solo`) to avoid async loop collisions |

---

## Engineering Log: 5 Production Bugs Solved

Deploying a distributed system from local sandbox to Railway cloud containers exposed real infrastructure failures. Below is the complete engineering log.

---

### Bug 1 — Async Event Loop Startup Deadlock

**Symptom:**  
FastAPI froze on deployment, hanging indefinitely at:
```
Waiting for application startup
```
Container was killed by Railway's health checker timeout before binding to its port.

**Root Cause:**  
The `@app.on_event("startup")` hook ran a blocking database verification call:
```python
await conn.run_sync(Base.metadata.create_all)
```
This locked Uvicorn's single async event loop thread during initialization, preventing the container from completing startup and binding to its network socket.

**Fix:**  
Since all schemas were already initialized via a dedicated migration script (`force_neon_init.py`), the startup verification was redundant. Removed the blocking call entirely — container reached ready state in milliseconds.

---

### Bug 2 — Serverless Connection Pool Deadlocks via PgBouncer

**Symptom:**  
Database query channels silently locked mid-session during data processing, with no standard DB exceptions thrown.

**Root Cause:**  
Neon routes connections through **PgBouncer in transaction pooling mode**. The `asyncpg` driver generates server-side prepared statements by default. In transaction pooling, sequential operations within a session are distributed across different backend clusters — so when the driver referenced a prepared statement on an instance that didn't generate it, the session deadlocked silently.

**Fix:**  
Disabled asyncpg's prepared statement cache entirely in `database.py`:
```python
engine = create_async_engine(
    DATABASE_URL,
    prepared_statement_name_cache_size=0
)
```

---

### Bug 3 — Cross-OS Cache Corruption (WinError 3)

**Symptom:**  
Celery Beat crashed on Railway with:
```
WinError 3: The system cannot find the path specified
```

**Root Cause:**  
Celery Beat generates local state tracker binaries (`celerybeat-schedule.dat`, `.bak`) during development. On a Windows machine, these binaries embedded absolute Windows paths (`D:\Datasets\Notebooks\...`). These files were accidentally committed to Git — when the Linux Railway container read them, it threw fatal path exceptions.

**Fix:**  
Removed the binaries from Git index and added explicit exclusions to `.gitignore`:
```
# .gitignore
sentimentops-backend/celerybeat-schedule*
pipeline.log
__pycache__/
```

---

### Bug 4 — Container Namespace Collision (ModuleNotFoundError)

**Symptom:**  
The web container booted correctly. The worker container crashed immediately:
```
ModuleNotFoundError: No module named 'database'
```

**Root Cause:**  
The codebase lives inside a subfolder (`sentimentops-backend/`). The Dockerfile set `WORKDIR /app`. When Celery spawned internal threads, it reset its lookup scope to `/app`, losing visibility of sibling modules like `database.py` inside the subfolder.

**Fix:**  
Set `PYTHONPATH` as an environment variable in the Railway deployment portal:
```
PYTHONPATH = /app/sentimentops-backend
```
This bound the subfolder to Python's module search path permanently, allowing clean direct start commands:
```
celery -A celery_app worker --loglevel=info -P solo
```

---

### Bug 5 — CORS Preflight Rejection (Blank Frontend)

**Symptom:**  
Backend logs showed clean operations. The React frontend was completely blank with browser console errors:
```
Response to preflight request doesn't pass access control check
```

**Root Cause:**  
Lovable generates dynamic sandbox subdomains (`*.lovableproject.com`) for frontend previews. The backend's strict CORS policy rejected the dynamic origin, blocking all client-side data fetches.

**Fix:**  
Updated CORS middleware to allow all origins (safe given the backend serves only aggregate read-only analytics):
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

## Project Structure

```
Sentiment_Analysis/
├── sentimentops-backend/
│   ├── app.py              # FastAPI routing and lifecycle
│   ├── database.py         # Async engine, session management
│   ├── models.py           # SQLAlchemy 2.0 schema (DeclarativeBase)
│   ├── worker.py           # Celery task definitions
│   ├── celery_app.py       # Celery + Beat configuration
│   └── force_neon_init.py  # One-time schema migration utility
├── Dockerfile
├── docker-compose.yml
├── railway.beat.toml       # Railway Beat service config
├── railway.worker.toml     # Railway Worker service config
├── Procfile
└── requirements.txt
```

---

## Key Architectural Decisions

**Why Celery + Beat instead of FastAPI BackgroundTasks?**  
FastAPI's built-in background tasks share the event loop with the API server. For 19K+ row processing, this would degrade API response times. Celery isolates compute entirely — the API issues a `202 Accepted` instantly and the worker handles the rest independently.

**Why `-P solo` for the worker?**  
Celery's default multiprocessing prefork model conflicts with async code. Solo mode runs a single-threaded event loop inside the worker, preventing async context conflicts when writing to the async SQLAlchemy session.

**Why dual-role Redis?**  
Using Upstash Redis as both the Celery message broker and the analytics cache eliminates a second managed service, reduces cold-start latency, and keeps the architecture's moving parts to a minimum.

---

## Production Status

| Service | Status |
|---|---|
| Sentiment-Web | ✅ ONLINE |
| Sentiment-Worker | ✅ ONLINE |
| Sentiment-Beat | ✅ ONLINE |
| Neon PostgreSQL | ✅ 19,000+ rows synced |
| Frontend | ✅ [sentimentops.lovable.app](https://sentimentops.lovable.app) |

---

*Built by [Lakshya Singh Kushwah](https://github.com/Laksh-tech)*
