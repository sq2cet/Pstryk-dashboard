from datetime import UTC, date, datetime, timedelta

import pytest
from sqlmodel import Session

from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.db import engine
from app.models import MeterReading
from app.services.cost import compute_day, kwh_between, materialise_day
from app.services.ingest import record_meter_reading, upsert_pstryk_prices


@pytest.fixture
def session():
    with Session(engine) as s:
        yield s


def _naive_utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


def _reading(ts: datetime, power_w: float, kwh: float | None = None) -> BleBoxReading:
    return BleBoxReading(ts_utc=ts, active_power_w=power_w, energy_kwh_total=kwh, raw={})


def test_kwh_between_uses_counter_diff_when_available() -> None:
    a = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 10, 0, 0),
        active_power_w=1000.0,
        energy_kwh_total=10.0,
    )
    b = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 10, 1, 0),
        active_power_w=2000.0,  # would suggest a different result if we used power
        energy_kwh_total=10.5,
    )
    # Counter says 0.5 kWh used. Power-based estimate would be ~0.025 kWh.
    assert kwh_between(a, b) == pytest.approx(0.5)


def test_kwh_between_falls_back_to_power_when_counter_missing() -> None:
    a = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 10, 0, 0),
        active_power_w=1000.0,
        energy_kwh_total=None,
    )
    b = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 11, 0, 0),
        active_power_w=2000.0,
        energy_kwh_total=None,
    )
    # avg power 1500 W over 1 hour = 1.5 kWh
    assert kwh_between(a, b) == pytest.approx(1.5)


def test_kwh_between_falls_back_when_counter_resets() -> None:
    a = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 10, 0, 0),
        active_power_w=1000.0,
        energy_kwh_total=10.0,
    )
    b = MeterReading(
        ts_utc=_naive_utc(2026, 4, 27, 11, 0, 0),
        active_power_w=1000.0,
        energy_kwh_total=0.5,  # reset
    )
    # counter regressed → fall back to power: 1 kWh
    assert kwh_between(a, b) == pytest.approx(1.0)


def test_kwh_between_returns_none_for_zero_or_negative_dt() -> None:
    ts = _naive_utc(2026, 4, 27, 10, 0, 0)
    a = MeterReading(ts_utc=ts, active_power_w=100.0, energy_kwh_total=1.0)
    b = MeterReading(ts_utc=ts, active_power_w=100.0, energy_kwh_total=1.0)
    assert kwh_between(a, b) is None


def test_compute_day_with_no_readings_is_zero(session: Session) -> None:
    totals = compute_day(session, date(2026, 4, 27), tz_name="UTC")
    assert totals.kwh == 0.0
    assert totals.cost_pln == 0.0


def test_compute_day_integrates_readings_against_hourly_price(session: Session) -> None:
    # In Europe/Warsaw, 2026-04-27 starts at 22:00 UTC on 2026-04-26 (CEST = UTC+2).
    # Use UTC timezone here for simplicity.
    upsert_pstryk_prices(
        session,
        [
            HourlyPrice(
                ts_utc=_naive_utc(2026, 4, 27, 10, 0, 0), price_pln_per_kwh=0.40, kind="historical"
            ),
            HourlyPrice(
                ts_utc=_naive_utc(2026, 4, 27, 11, 0, 0), price_pln_per_kwh=0.60, kind="historical"
            ),
        ],
    )

    base = _naive_utc(2026, 4, 27, 10, 0, 0)
    record_meter_reading(session, _reading(base, power_w=0.0, kwh=0.0))
    record_meter_reading(session, _reading(base + timedelta(minutes=30), power_w=0.0, kwh=0.5))
    record_meter_reading(session, _reading(base + timedelta(hours=1), power_w=0.0, kwh=1.0))
    record_meter_reading(
        session, _reading(base + timedelta(hours=1, minutes=30), power_w=0.0, kwh=2.0)
    )

    totals = compute_day(session, date(2026, 4, 27), tz_name="UTC")
    # Two intervals at 0.40 PLN (midpoints in hour 10): 0.5 kWh + 0.5 kWh = 1.0 kWh × 0.40 = 0.40
    # One interval at 0.60 PLN (midpoint in hour 11): 1.0 kWh × 0.60 = 0.60
    # Total: 2.0 kWh, 1.00 PLN, weighted avg 0.50
    assert totals.kwh == pytest.approx(2.0)
    assert totals.cost_pln == pytest.approx(1.0)
    assert totals.avg_price_pln_per_kwh == pytest.approx(0.5)


def test_materialise_day_upserts_aggregate_row(session: Session) -> None:
    upsert_pstryk_prices(
        session,
        [
            HourlyPrice(
                ts_utc=_naive_utc(2026, 4, 27, 10, 0, 0), price_pln_per_kwh=0.40, kind="historical"
            )
        ],
    )
    base = _naive_utc(2026, 4, 27, 10, 0, 0)
    record_meter_reading(session, _reading(base, power_w=0.0, kwh=0.0))
    record_meter_reading(session, _reading(base + timedelta(minutes=30), power_w=0.0, kwh=0.5))

    row = materialise_day(session, date(2026, 4, 27), tz_name="UTC")
    assert row.kwh == pytest.approx(0.5)
    assert row.cost_pln == pytest.approx(0.20)

    # Re-running upserts in place
    record_meter_reading(session, _reading(base + timedelta(hours=1), power_w=0.0, kwh=1.0))
    row = materialise_day(session, date(2026, 4, 27), tz_name="UTC")
    assert row.kwh == pytest.approx(1.0)
