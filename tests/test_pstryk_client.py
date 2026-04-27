import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.clients.pstryk import (
    UNIFIED_METRICS_PATH,
    PstrykAuthError,
    PstrykClient,
    PstrykRateLimitError,
    parse_hourly_prices,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "pstryk_unified.json").read_text())
URL = f"https://api.pstryk.pl{UNIFIED_METRICS_PATH}"


@pytest.fixture
def window() -> tuple[datetime, datetime]:
    start = datetime(2026, 4, 26, 22, 0, tzinfo=UTC)
    end = datetime(2026, 4, 27, 0, 0, tzinfo=UTC)
    return start, end


@respx.mock
async def test_fetch_unified_metrics_success(window: tuple[datetime, datetime]) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(200, json=FIXTURE))
    async with PstrykClient(api_key="sk-abc") as client:
        payload = await client.fetch_unified_metrics(*window)
    assert route.called
    request = route.calls.last.request
    # Auth header is the raw token without a Bearer/Token prefix.
    assert request.headers["authorization"] == "sk-abc"
    # Required query params are present.
    assert request.url.params["metrics"] == "pricing,meter_values,cost"
    assert request.url.params["resolution"] == "hour"
    assert "summary" in payload


@respx.mock
async def test_fetch_unified_metrics_auth_error(window: tuple[datetime, datetime]) -> None:
    respx.get(URL).mock(return_value=httpx.Response(403, text="bad key"))
    async with PstrykClient(api_key="bad") as client:
        with pytest.raises(PstrykAuthError):
            await client.fetch_unified_metrics(*window)


@respx.mock
async def test_fetch_unified_metrics_429_backoff(
    window: tuple[datetime, datetime],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    monkeypatch.setattr("app.clients.pstryk.asyncio.sleep", fake_sleep)

    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(429),
            httpx.Response(429),
            httpx.Response(200, json=FIXTURE),
        ],
    )
    async with PstrykClient(api_key="k") as client:
        payload = await client.fetch_unified_metrics(*window)
    assert route.call_count == 3
    assert sleeps == [2.0, 4.0]
    assert "frames" in payload


@respx.mock
async def test_fetch_unified_metrics_429_gives_up(
    window: tuple[datetime, datetime],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def noop(_s: float) -> None:
        return None

    monkeypatch.setattr("app.clients.pstryk.asyncio.sleep", noop)

    respx.get(URL).mock(return_value=httpx.Response(429))
    async with PstrykClient(api_key="k") as client:
        with pytest.raises(PstrykRateLimitError):
            await client.fetch_unified_metrics(*window)


def test_parse_hourly_prices_extracts_price_gross() -> None:
    parsed = parse_hourly_prices(FIXTURE)
    assert len(parsed) == 3
    assert all(p.ts_utc.tzinfo is None for p in parsed)
    assert [p.price_pln_per_kwh for p in parsed] == [0.5328, 0.6010, 0.7500]
    # The first two frames are in the past, the 2099 frame is in the future.
    assert [p.kind for p in parsed] == ["historical", "historical", "forecast"]


def test_parse_hourly_prices_extracts_meter_and_cost() -> None:
    parsed = parse_hourly_prices(FIXTURE)
    # First frame in fixture has full meterValues + cost.
    assert parsed[0].kwh_import == 1.5
    assert parsed[0].kwh_export == 0.0
    assert parsed[0].cost_pln == pytest.approx(0.7992)
    # Remaining frames carry no meter/cost — fields stay None.
    assert parsed[1].kwh_import is None
    assert parsed[1].cost_pln is None
    assert parsed[2].kwh_import is None
    assert parsed[2].cost_pln is None


def test_parse_hourly_prices_falls_back_to_full_price() -> None:
    payload = {
        "frames": [
            {
                "start": "2026-04-26T22:00:00Z",
                "metrics": {"pricing": {"full_price": 0.45}},
            }
        ],
    }
    parsed = parse_hourly_prices(payload)
    assert len(parsed) == 1
    assert parsed[0].price_pln_per_kwh == 0.45


def test_parse_hourly_prices_treats_is_live_as_historical() -> None:
    payload = {
        "frames": [
            {
                "start": "2999-01-01T00:00:00Z",  # in the future...
                "is_live": True,  # ...but flagged as the current hour
                "metrics": {"pricing": {"price_gross": 0.50}},
            }
        ],
    }
    parsed = parse_hourly_prices(payload)
    assert parsed[0].kind == "historical"


def test_parse_hourly_prices_skips_unparseable_frames() -> None:
    payload = {
        "frames": [
            {"start": "not-a-timestamp", "metrics": {"pricing": {"price_gross": 1.0}}},
            {"start": "2026-04-26T22:00:00Z", "metrics": {"pricing": {}}},
            {"start": "2026-04-26T22:00:00Z"},
        ]
    }
    assert parse_hourly_prices(payload) == []
