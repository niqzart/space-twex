from redis.asyncio import Redis

db: Redis = Redis(decode_responses=True)  # type: ignore[type-arg]
