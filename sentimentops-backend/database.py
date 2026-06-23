import os
from datetime import datetime
from typing import List
from pydantic import BaseModel
from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from dotenv import load_dotenv

current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env.production"))

# Force-inject your brand-new Neon connection string
# Swap the generic prefix to use 'postgresql+asyncpg' for async runtime execution
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable not set")

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args={"ssl": True}
)

class Base(DeclarativeBase):
    pass

# --- Pydantic Schemas ---
class ReviewItem(BaseModel):
    text: str
    date: str  

class ReviewBatch(BaseModel):
    reviews: List[ReviewItem]  

# --- PostgreSQL Unified Model ---
class Sentiment(Base):
    __tablename__ = "sentiments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String(1000), nullable=False) # Maps to VARCHAR(1000)
    label: Mapped[str] = mapped_column(String(50), nullable=False)   # Maps to VARCHAR(50)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

sessionlocal = async_sessionmaker(
    bind=engine, 
    autoflush=False, 
    autocommit=False, 
    expire_on_commit=False 
)

async def get_db():
    async with sessionlocal() as db:
        yield db