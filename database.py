import os
import ssl
from datetime import datetime
from typing import List, AsyncIterator
from pydantic import BaseModel
from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from dotenv import load_dotenv

# Load local environment variables (ignored in production/Railway environments)
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("CRITICAL ERROR: DATABASE_URL environment variable is missing!")

# Handle SSL Arguments specifically tailored for cloud providers like Aiven
connect_args = {}
if "aivencloud.com" in DATABASE_URL:
    # Create an unverified SSL context to prevent certificate validation errors over public paths
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # aiomysql requires the ssl parameter bound to a context object
    connect_args["ssl"] = ssl_context

# Initialize our high-performance asynchronous connection engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,  # SDE-1 Standard: Automatically tests disconnected sockets
    pool_recycle=3600    # Prevents Aiven from silently dropping idle connections
)

class Base(DeclarativeBase):
    pass

# --- Pydantic Data Validation Schemas (Contracts) ---
class ReviewItem(BaseModel):
    text: str
    date: str  

class ReviewBatch(BaseModel):
    reviews: List[ReviewItem]  

class SummaryResponse(BaseModel):
    Review_Count: int
    Positive_Reviews: int
    Negative_Reviews: int
    Neutral_Reviews: int
    Overall_Positivity_Rate: float
    Score_Correlation: List[dict]
    Average_Scores: List[dict]
    Review_Length_Analysis: List[dict]
    Urban_Reviews: List[dict]

# --- Database Core Models ---
class Sentiment(Base):
    __tablename__ = "sentiments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String(1000), nullable=False)
    label: Mapped[str] = mapped_column(String(50), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

# Configure the thread-safe session generator
sessionlocal = async_sessionmaker(
    bind=engine, 
    autoflush=False, 
    autocommit=False, 
    expire_on_commit=False 
)

# FIXED: Dependency function now correctly yields the session handler instance
async def get_db() -> AsyncIterator[AsyncSession]:
    async with sessionlocal() as db:
        yield db  # <--- Yielding the database session fix