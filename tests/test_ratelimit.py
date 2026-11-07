import asyncio
import time

from dbdelay.ratelimit import RateLimiter


async def test_burst_then_throttle() -> None:
    limiter = RateLimiter(per_minute=600, burst=3)  # 10/s
    start = time.monotonic()
    for _ in range(3):
        await limiter.acquire()
    assert time.monotonic() - start < 0.05
    for _ in range(3):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    assert 0.25 <= elapsed < 0.6


async def test_concurrent_acquires_respect_rate() -> None:
    limiter = RateLimiter(per_minute=1200, burst=1)  # 20/s
    start = time.monotonic()
    await asyncio.gather(*(limiter.acquire() for _ in range(6)))
    assert time.monotonic() - start >= 0.24
