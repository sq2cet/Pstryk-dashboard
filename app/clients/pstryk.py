"""Async client for the Pstryk integrations API.

The exact response schema of `/integrations/unified-metrics/` is not in
the public docs (Swagger requires login). The parser below handles the
common shapes seen in community Home Assistant integrations and falls
back gracefully when fields are renamed. Once a real key produces a
sample response, tighten the parser to match.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.pstryk.pl"
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
                "Authorization": f"Bearer {api_key}",
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
    ) -> dict:
        """Fetch /integrations/unified-metrics/ for the given UTC window."""
        params = {
            "window_start": _isoformat_utc(window_start),
            "window_end": _isoformat_utc(window_end),
            "resolution": resolution,
        }
        return await self._get_json("/integrations/unified-metrics/", params)

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
    """Tolerant parser for the unified-metrics payload.

    Looks for an iterable of hourly entries under any of the keys we've
    seen across community implementations (`prices`, `frames`, `entries`,
    `data`). Each entry is expected to expose a timestamp and a
    PLN/kWh price under one of several common names.
    """
    rows = (
        payload.get("prices")
        or payload.get("frames")
        or payload.get("entries")
        or payload.get("data")
        or []
    )
    parsed: list[HourlyPrice] = []
    for row in rows:
        ts = _parse_ts(row)
        price = _parse_price(row)
        if ts is None or price is None:
            continue
        kind = "forecast" if _is_forecast(row, ts) else "historical"
        parsed.append(HourlyPrice(ts_utc=ts, price_pln_per_kwh=price, kind=kind))
    parsed.sort(key=lambda p: p.ts_utc)
    return parsed


def _isoformat_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(row: dict) -> datetime | None:
    for key in ("timestamp", "start", "time", "ts", "from", "datetime"):
        raw = row.get(key)
        if isinstance(raw, str):
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt.astimezone(UTC).replace(tzinfo=None)
            except ValueError:
                continue
    return None


def _parse_price(row: dict) -> float | None:
    for key in ("price_gross", "gross_price", "price", "value", "pln_per_kwh"):
        raw = row.get(key)
        if isinstance(raw, (int, float)):
            return float(raw)
    return None


def _is_forecast(row: dict, ts: datetime) -> bool:
    if "is_forecast" in row:
        return bool(row["is_forecast"])
    if isinstance(row.get("kind"), str):
        return row["kind"].lower() in {"forecast", "future"}
    return ts > datetime.now(UTC).replace(tzinfo=None)
