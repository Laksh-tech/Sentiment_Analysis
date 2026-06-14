# force_neon_init.py
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from database import Base

# Neon direct secure async URL destination
ASYNC_NEON_URL = "postgresql+asyncpg://neondb_owner:npg_c8T3HDCUuibR@ep-spring-band-aomqd54n-pooler.c-2.ap-southeast-1.aws.neon.tech/neondb"

async def main():
    print("⏳ Establishing direct async connection with Neon Postgres cluster...")
    cloud_engine = create_async_engine(ASYNC_NEON_URL, echo=True, connect_args={"ssl": True})
    
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
    