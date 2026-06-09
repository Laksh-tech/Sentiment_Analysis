from datetime import datetime
from typing import List
from pydantic import BaseModel
from sqlalchemy import String, Float, Integer, DateTime
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import os
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "mysql+aiomysql://root:root@localhost/sentiment_db"  # local fallback
)
engine = create_async_engine(DATABASE_URL, echo=False)  # echo=False in prod

class Base(DeclarativeBase):
    pass

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
    Urgent_Reviews: List[dict]

# FIX: Upgraded to modern SQLAlchemy 2.0 
class Sentiment(Base):
    __tablename__ = "sentiments"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text: Mapped[str] = mapped_column(String(1000), nullable=False)
    label: Mapped[str] = mapped_column(String(50), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

sessionlocal = async_sessionmaker(
    bind=engine, 
    autoflush=False, 
    autocommit=False, 
    expire_on_commit=False # FIX: Prevents losing object state after commit
)

async def get_db():
    async with sessionlocal() as db:
        yield db
