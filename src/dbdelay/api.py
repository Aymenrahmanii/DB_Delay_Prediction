"""Async client for the DB Timetables API (/plan, /fchg, /rchg, /station)."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
)

from dbdelay.config import BERLIN, Settings
from dbdelay.ratelimit import RateLimiter

RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class Response:
    path: str
    status: int
    body: bytes
    attempts: int
    elapsed_s: float


class RetryableStatusError(Exception):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(f"HTTP {response.status_code}")
        self.response = response


def _is_retryable(exc: BaseException) -> bool:
    return isinstance(exc, RetryableStatusError | httpx.TransportError)


def plan_path(eva: str, slot: datetime) -> str:
    """Path for the planned stops of one hour; `slot` is converted to Berlin time."""
    local = slot.astimezone(BERLIN)
    return f"/plan/{eva}/{local:%y%m%d}/{local:%H}"


class TimetablesClient:
    def __init__(
        self,
        settings: Settings,
        limiter: RateLimiter,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._settings = settings
        self._limiter = limiter
        self._client = httpx.AsyncClient(
            base_url=settings.base_url,
            headers={
                "DB-Client-Id": settings.db_client_id.get_secret_value(),
                "DB-Api-Key": settings.db_api_key.get_secret_value(),
                "Accept": "application/xml",
                "User-Agent": "dbdelay-collector/0.1",
            },
            timeout=settings.request_timeout_s,
            limits=httpx.Limits(max_connections=10, max_keepalive_connections=10),
            transport=transport,
        )

    async def __aenter__(self) -> TimetablesClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get(self, path: str) -> Response:
        """GET with rate limiting and retries on 429/5xx/network errors.

        Non-retryable statuses (e.g. 401, 404) are returned, not raised. After the last
        attempt a retryable status is returned too; a transport error is raised.
        """
        attempts = 0
        started = time.monotonic()
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._settings.max_attempts),
            wait=wait_random_exponential(multiplier=2, max=30),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        )
        try:
            async for attempt in retrying:
                with attempt:
                    attempts += 1
                    await self._limiter.acquire()
                    resp = await self._client.get(path)
                    if resp.status_code in RETRY_STATUS:
                        raise RetryableStatusError(resp)
        except RetryableStatusError as exc:
            resp = exc.response
        return Response(
            path=path,
            status=resp.status_code,
            body=resp.content,
            attempts=attempts,
            elapsed_s=time.monotonic() - started,
        )

    async def plan(self, eva: str, slot: datetime) -> Response:
        return await self.get(plan_path(eva, slot))

    async def fchg(self, eva: str) -> Response:
        return await self.get(f"/fchg/{eva}")

    async def rchg(self, eva: str) -> Response:
        return await self.get(f"/rchg/{eva}")

    async def station(self, pattern: str) -> Response:
        return await self.get(f"/station/{quote(pattern, safe='')}")
