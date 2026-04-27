"""Async client for the Pstryk integrations API.

Matches the OpenAPI spec at https://api.pstryk.pl/integrations/schema/.
The single endpoint we use is
`/integrations/meter-data/unified-metrics/` (the spec calls older
single-metric endpoints "deprecated" and routes them through this one).

Auth: the Pstryk API key is sent as `Authorization: <key>` directly,
without a `Bearer` or `Token` scheme prefix. This is the
TokenAuthentication scheme declared in the spec and the format shown in
the spec's request example (`Authorization: sk-your-token-here`).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.pstryk.pl"
UNIFIED_METRICS_PATH = "/integrations/meter-data/unified-metrics/"
DEFAULT_TIMEOUT = 20.0
MAX_RETRIES_ON_429 = 4


class PstrykAPIError(Exception):
    pass


class PstrykAuthError(PstrykAPIError):
    pass


class PstrykRateLimitError(PstrykAPIError):
    pass


@dataclass(frozen=True)
class HourlyPrice:
    ts_utc: datetime
    price_pln_per_kwh: float
    kind: str  # 'historical' | 'forecast'


class PstrykClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            headers={
                "Authorization": api_key,
                "Accept": "application/json",
            },
        )

    async def __aenter__(self) -> PstrykClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch_unified_metrics(
        self,
        window_start: datetime,
        window_end: datetime,
        resolution: str = "hour",
        metrics: str = "pricing",
    ) -> dict:
        """Fetch the unified-metrics endpoint for the given UTC window.

        `metrics` is a comma-separated subset of
        {`meter_values`, `cost`, `carbon`, `pricing`}. Defaults to
        `pricing` which is what the dashboard's tariff backfill needs.
        """
        params = {
            "metrics": metrics,
            "resolution": resolution,
            "window_start": _isoformat_utc(window_start),
            "window_end": _isoformat_utc(window_end),
        }
        return await self._get_json(UNIFIED_METRICS_PATH, params)

    async def _get_json(self, path: str, params: dict) -> dict:
        backoff = 2.0
        for attempt in range(MAX_RETRIES_ON_429 + 1):
            response = await self._client.get(path, params=params)
            if response.status_code == 429:
                if attempt == MAX_RETRIES_ON_429:
                    raise PstrykRateLimitError(
                        f"429 Too Many Requests after {attempt + 1} attempts"
                    )
                logger.warning(
                    "Pstryk 429 on %s (attempt %d/%d) — backing off %.1fs",
                    path,
                    attempt + 1,
                    MAX_RETRIES_ON_429,
                    backoff,
                )
                await asyncio.sleep(backoff)
                backoff *= 2
                continue
            if response.status_code in (401, 403):
                raise PstrykAuthError(f"{response.status_code} {response.text[:200]}")
            response.raise_for_status()
            return response.json()
        raise PstrykAPIError("unreachable")


def parse_hourly_prices(payload: dict) -> list[HourlyPrice]:
    """Parse the gross PLN/kWh price out of `frames[].metrics.pricing`.

    The Pstryk API returns hourly frames with a nested `pricing` object
    containing `price_gross` (the consumer-facing price including
    distribution + service + VAT + excise). Frames whose `start` is in
    the future are tagged `forecast`; everything else is `historical`.
    Frames carrying `is_live: true` are the current hour and are also
    treated as historical (the price for the current hour is fixed).
    """
    frames = payload.get("frames") or []
    parsed: list[HourlyPrice] = []
    now = datetime.now(UTC).replace(tzinfo=None)
    for frame in frames:
        start = frame.get("start")
        if not isinstance(start, str):
            continue
        try:
            ts = (
                datetime.fromisoformat(start.replace("Z", "+00:00"))
                .astimezone(UTC)
                .replace(tzinfo=None)
            )
        except ValueError:
            continue
        pricing = (frame.get("metrics") or {}).get("pricing") or {}
        price = pricing.get("price_gross")
        if price is None:
            price = pricing.get("full_price")
        if not isinstance(price, (int, float)):
            continue
        kind = "forecast" if (ts > now and not frame.get("is_live")) else "historical"
        parsed.append(HourlyPrice(ts_utc=ts, price_pln_per_kwh=float(price), kind=kind))
    parsed.sort(key=lambda p: p.ts_utc)
    return parsed


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
