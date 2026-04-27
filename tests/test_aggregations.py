from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session

from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.db import engine
from app.services.aggregations import aggregate_range, period_totals, resolve_window
from app.services.ingest import record_meter_reading, upsert_pstryk_prices


@pytest.fixture
def session():
    with Session(engine) as s:
        yield s


def _ts(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


def _seed_two_hours(session: Session) -> None:
    upsert_pstryk_prices(
        session,
        [
            HourlyPrice(_ts(2026, 4, 27, 10, 0, 0), 0.40, "historical"),
            HourlyPrice(_ts(2026, 4, 27, 11, 0, 0), 0.60, "historical"),
        ],
    )
    base = _ts(2026, 4, 27, 10, 0, 0)
    record_meter_reading(
        session,
        BleBoxReading(ts_utc=base, active_power_w=0.0, energy_kwh_total=0.0, raw={}),
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(minutes=30), active_power_w=0.0, energy_kwh_total=0.5, raw={}
        ),
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(hours=1), active_power_w=0.0, energy_kwh_total=1.0, raw={}
        ),
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(hours=1, minutes=30),
            active_power_w=0.0,
            energy_kwh_total=2.0,
            raw={},
        ),
    )


def test_hourly_aggregate_over_two_hours(session: Session) -> None:
    _seed_two_hours(session)
    rows, totals, cum, cum_kwh = aggregate_range(
        session,
        _ts(2026, 4, 27, 10, 0, 0),
        _ts(2026, 4, 27, 12, 0, 0),
        "hour",
        "UTC",
    )
    assert len(rows) == 2
    assert rows[0].kwh == pytest.approx(1.0)
    assert rows[0].cost_pln == pytest.approx(0.40)
    assert rows[1].kwh == pytest.approx(1.0)
    assert rows[1].cost_pln == pytest.approx(0.60)
    assert totals["kwh"] == pytest.approx(2.0)
    assert totals["cost_pln"] == pytest.approx(1.0)
    assert totals["avg_price_pln_per_kwh"] == pytest.approx(0.5)  # weighted
    assert totals["min_price_pln_per_kwh"] == 0.40
    assert totals["max_price_pln_per_kwh"] == 0.60
    assert cum == pytest.approx([0.40, 1.00])
    assert cum_kwh == pytest.approx([1.0, 2.0])


def test_daily_aggregate_collapses_hours_in_local_tz(session: Session) -> None:
    _seed_two_hours(session)
    rows, totals, cum, cum_kwh = aggregate_range(
        session,
        _ts(2026, 4, 27, 10, 0, 0),
        _ts(2026, 4, 27, 12, 0, 0),
        "day",
        "UTC",
    )
    assert len(rows) == 1
    assert rows[0].kwh == pytest.approx(2.0)
    assert rows[0].cost_pln == pytest.approx(1.0)
    assert rows[0].min_price_pln_per_kwh == 0.40
    assert rows[0].max_price_pln_per_kwh == 0.60


def test_period_totals_sums_pstryk_first(session: Session) -> None:
    upsert_pstryk_prices(
        session,
        [
            HourlyPrice(
                ts_utc=_ts(2026, 4, 27, 10, 0, 0),
                price_pln_per_kwh=0.50,
                kind="historical",
                kwh_import=2.0,
                cost_pln=1.10,
            ),
            HourlyPrice(
                ts_utc=_ts(2026, 4, 27, 11, 0, 0),
                price_pln_per_kwh=0.60,
                kind="historical",
                kwh_import=1.5,
                cost_pln=0.90,
            ),
        ],
    )
    totals = period_totals(
        session,
        _ts(2026, 4, 27, 10, 0, 0),
        _ts(2026, 4, 27, 12, 0, 0),
    )
    assert totals["kwh"] == pytest.approx(3.5)
    assert totals["cost_pln"] == pytest.approx(2.0)
    assert totals["avg_price_pln_per_kwh"] == pytest.approx(2.0 / 3.5)


def test_resolve_window_today_uses_local_midnight() -> None:
    from zoneinfo import ZoneInfo

    tz = ZoneInfo("Europe/Warsaw")
    start, end, default_res = resolve_window("today", tz)
    assert default_res == "hour"
    assert (end - start) == timedelta(days=1)


def test_resolve_window_custom_requires_dates() -> None:
    from zoneinfo import ZoneInfo

    with pytest.raises(ValueError):
        resolve_window("custom", ZoneInfo("UTC"))
