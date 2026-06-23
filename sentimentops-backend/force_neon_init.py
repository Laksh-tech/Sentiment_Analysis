# force_neon_init.py
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from database import Base
from dotenv import load_dotenv
import os
# Neon direct secure async URL destination
current_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(current_dir, ".env.production"))

ASYNC_NEON_URL = os.getenv("DATABASE_URL")
if not ASYNC_NEON_URL:
    raise RuntimeError("DATABASE_URL environment variable not set")

#print(ASYNC_NEON_URL) #working
async def main():
    print("⏳ Establishing direct async connection with Neon Postgres cluster...")
    cloud_engine = create_async_engine(ASYNC_NEON_URL, echo=True, connect_args={"ssl": True}) # pyright: ignore[reportArgumentType] 
    
    try:
        async with cloud_engine.begin() as conn:
            print("🚀 Executing table initialization routines on Neon...")
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)
        print("\n========================================================")
        print("✅ SUCCESS: The 'sentiments' table is now LIVE on Neon!")
        print("========================================================\n")
    except Exception as e:
        print(f"❌ CONNECTION FAILURE: {str(e)}")
    finally:
        await cloud_engine.dispose()

if __name__ == "__main__":
    asyncio.run(main())
    