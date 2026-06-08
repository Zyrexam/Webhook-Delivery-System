import os
from dotenv import load_dotenv
import redis.asyncio as aioredis

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

redis = aioredis.from_url(REDIS_URL, decode_responses=True)
