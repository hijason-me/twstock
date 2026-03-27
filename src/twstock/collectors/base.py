"""
Shared HTTP client with rate-limiting and automatic retry.
All collectors should create clients via `build_client()`.
"""
import asyncio
import logging

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

_RETRY_EXCEPTIONS = (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError)


def _make_retry():
    return retry(
        reraise=True,
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(_RETRY_EXCEPTIONS),
        before_sleep=lambda rs: logger.warning(
            "Retry %d: %s", rs.attempt_number, rs.outcome.exception()
        ),
    )


retry_request = _make_retry()


def build_client(**kwargs) -> httpx.AsyncClient:
    """Return a pre-configured async HTTP client."""
    defaults = dict(
        timeout=30.0,
        headers={"User-Agent": "TWStock-Pipeline/2.0 (+https://github.com/hijason-me/twstock)"},
        follow_redirects=True,
    )
    defaults.update(kwargs)
    return httpx.AsyncClient(**defaults)


async def throttle(seconds: float) -> None:
    if seconds > 0:
        await asyncio.sleep(seconds)
