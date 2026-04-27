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
    # Without a reading, the per-phase / diagnostics sections must not
    # render and the "Now" section's Power cell shows the muted dash.
    assert "Per phase" not in response.text
    assert "Diagnostics" not in response.text
    assert "This month" in response.text  # totals section still rendered


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


def test_range_endpoint_returns_buckets_totals_cumulative() -> None:
    _configure()
    with TestClient(app) as client:
        response = client.get("/api/charts/range?range=today&resolution=hour")
    assert response.status_code == 200
    body = response.json()
    assert body["range"] == "today"
    assert body["resolution"] == "hour"
    assert "buckets" in body
    assert "totals" in body
    assert "cumulative_cost_pln" in body
    assert len(body["cumulative_cost_pln"]) == len(body["buckets"])
    for k in ("kwh", "cost_pln", "avg_price_pln_per_kwh", "bucket_count"):
        assert k in body["totals"]


def test_range_endpoint_rejects_bad_resolution() -> None:
    _configure()
    with TestClient(app) as client:
        response = client.get("/api/charts/range?range=today&resolution=fortnight")
    assert response.status_code == 400


def test_range_endpoint_rejects_custom_without_dates() -> None:
    _configure()
    with TestClient(app) as client:
        response = client.get("/api/charts/range?range=custom")
    assert response.status_code == 400


def test_live_power_endpoint_reflects_buffer() -> None:
    _configure()
    state.reset_buffers()
    from app.clients.blebox import BleBoxReading, PhaseReading

    now = datetime.now(UTC).replace(tzinfo=None)
    state.set_last_reading(
        BleBoxReading(
            ts_utc=now,
            active_power_w=521.0,
            energy_kwh_total=109.5,
            raw={},
            phase_l1=PhaseReading(active_power_w=151.0),
            phase_l2=PhaseReading(active_power_w=267.0),
            phase_l3=PhaseReading(active_power_w=103.0),
        )
    )

    with TestClient(app) as client:
        response = client.get("/api/charts/live-power?minutes=10")
    assert response.status_code == 200
    body = response.json()
    assert len(body["ts"]) == 1
    assert body["total_w"] == [521.0]
    assert body["l1_w"] == [151.0]
    assert body["l2_w"] == [267.0]
    assert body["l3_w"] == [103.0]
    state.reset_buffers()


def test_cheapest_hours_partial_renders() -> None:
    _configure()
    now_hour = datetime.now(UTC).replace(minute=0, second=0, microsecond=0, tzinfo=None)
    with Session(engine) as s:
        upsert_pstryk_prices(
            s,
            [
                HourlyPrice(now_hour + timedelta(hours=h), 0.5 + 0.05 * h, "forecast")
                for h in range(0, 24)
            ],
        )

    with TestClient(app) as client:
        response = client.get("/partials/cheapest-hours")
    assert response.status_code == 200
    assert "Cheapest" in response.text
    assert "Most expensive" in response.text
