import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from app.clients.pstryk import (
    PstrykAuthError,
    PstrykClient,
    PstrykRateLimitError,
    parse_hourly_prices,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "pstryk_unified.json").read_text())


@pytest.fixture
def window() -> tuple[datetime, datetime]:
    start = datetime(2026, 4, 27, 0, 0, tzinfo=UTC)
    end = datetime(2026, 4, 28, 0, 0, tzinfo=UTC)
    return start, end


@respx.mock
async def test_fetch_unified_metrics_success(window: tuple[datetime, datetime]) -> None:
    route = respx.get("https://api.pstryk.pl/integrations/unified-metrics/").mock(
        return_value=httpx.Response(200, json=FIXTURE),
    )
    async with PstrykClient(api_key="k") as client:
        payload = await client.fetch_unified_metrics(*window)
    assert route.called
    assert payload["balance_pln"] == 12.34


@respx.mock
async def test_fetch_unified_metrics_auth_error(window: tuple[datetime, datetime]) -> None:
    respx.get("https://api.pstryk.pl/integrations/unified-metrics/").mock(
        return_value=httpx.Response(401, text="bad key"),
    )
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

    route = respx.get("https://api.pstryk.pl/integrations/unified-metrics/").mock(
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
    assert payload["balance_pln"] == 12.34


@respx.mock
async def test_fetch_unified_metrics_429_gives_up(
    window: tuple[datetime, datetime],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.clients.pstryk.asyncio.sleep", lambda _s: _noop())

    respx.get("https://api.pstryk.pl/integrations/unified-metrics/").mock(
        return_value=httpx.Response(429),
    )
    async with PstrykClient(api_key="k") as client:
        with pytest.raises(PstrykRateLimitError):
            await client.fetch_unified_metrics(*window)


async def _noop() -> None:
    return None


def test_parse_hourly_prices_classifies_kind() -> None:
    parsed = parse_hourly_prices(FIXTURE)
    assert len(parsed) == 4
    assert all(p.ts_utc.tzinfo is None for p in parsed)
    assert [p.kind for p in parsed] == ["historical", "historical", "historical", "forecast"]
    assert [p.price_pln_per_kwh for p in parsed] == [0.42, 0.41, 0.40, 0.55]


def test_parse_hourly_prices_tolerates_alternate_keys() -> None:
    payload = {
        "frames": [
            {"timestamp": "2026-01-01T00:00:00Z", "value": 0.50},
            {"time": "2026-01-01T01:00:00+00:00", "pln_per_kwh": 0.60, "kind": "forecast"},
        ]
    }
    parsed = parse_hourly_prices(payload)
    assert len(parsed) == 2
    assert parsed[0].price_pln_per_kwh == 0.50
    assert parsed[1].price_pln_per_kwh == 0.60
    assert parsed[1].kind == "forecast"


def test_parse_hourly_prices_skips_unparseable() -> None:
    payload = {"prices": [{"foo": "bar"}, {"start": "2026-01-01T00:00:00Z"}]}
    assert parse_hourly_prices(payload) == []
