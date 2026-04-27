from datetime import UTC, datetime, timedelta

from fastapi.testclient import TestClient
from sqlmodel import Session

from app import state
from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.db import engine
from app.main import app
from app.services import settings_service as svc
from app.services.ingest import record_meter_reading, upsert_pstryk_prices


def _configure(api_key: str = "k", host: str = "1.2.3.4") -> None:
    with Session(engine) as s:
        svc.set_value(s, svc.PSTRYK_API_KEY, api_key)
        svc.set_value(s, svc.BLEBOX_HOST, host)


def test_live_tile_no_data_yet() -> None:
    state.set_last_reading(None)
    _configure()
    with TestClient(app) as client:
        response = client.get("/partials/live")
    assert response.status_code == 200
    assert "no reading yet" in response.text
    assert "Today" in response.text


def test_live_tile_with_cached_reading() -> None:
    _configure()
    state.set_last_reading(
        BleBoxReading(
            ts_utc=datetime.now(UTC).replace(tzinfo=None),
            active_power_w=521.0,
            energy_kwh_total=109.443,
            raw={},
        )
    )
    with Session(engine) as s:
        upsert_pstryk_prices(
            s,
            [
                HourlyPrice(
                    ts_utc=datetime.now(UTC).replace(
                        minute=0, second=0, microsecond=0, tzinfo=None
                    ),
                    price_pln_per_kwh=0.5328,
                    kind="historical",
                )
            ],
        )

    with TestClient(app) as client:
        response = client.get("/partials/live")
    assert response.status_code == 200
    assert "521" in response.text
    assert "0.5328" in response.text
    state.set_last_reading(None)


def test_hourly_chart_returns_well_formed_series() -> None:
    _configure()
    now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    with Session(engine) as s:
        upsert_pstryk_prices(
            s,
            [
                HourlyPrice(
                    ts_utc=now_hour - timedelta(hours=2), price_pln_per_kwh=0.5, kind="historical"
                ),
                HourlyPrice(
                    ts_utc=now_hour - timedelta(hours=1), price_pln_per_kwh=0.6, kind="historical"
                ),
                HourlyPrice(ts_utc=now_hour, price_pln_per_kwh=0.7, kind="historical"),
            ],
        )
        record_meter_reading(
            s,
            BleBoxReading(
                ts_utc=now_hour - timedelta(minutes=30),
                active_power_w=0.0,
                energy_kwh_total=10.0,
                raw={},
            ),
        )
        record_meter_reading(
            s,
            BleBoxReading(
                ts_utc=now_hour - timedelta(minutes=15),
                active_power_w=0.0,
                energy_kwh_total=10.4,
                raw={},
            ),
        )

    with TestClient(app) as client:
        response = client.get("/api/charts/hourly?hours=3&forecast_hours=0")
    body = response.json()
    assert response.status_code == 200
    assert body["history_hours"] == 3
    assert body["forecast_hours"] == 0
    assert len(body["series"]) == 3
    is_now_count = sum(1 for p in body["series"] if p["is_now"])
    assert is_now_count == 1
    # At least one bucket has a price; at least one has kwh.
    assert any(p["price_pln_per_kwh"] is not None for p in body["series"])
    assert any(p["kwh"] is not None for p in body["series"])


def test_hourly_chart_clamps_invalid_hours_param() -> None:
    _configure()
    with TestClient(app) as client:
        response = client.get("/api/charts/hourly?hours=0")
    assert response.status_code == 422
