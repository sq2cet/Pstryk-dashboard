from datetime import UTC, datetime, timedelta

import pytest
from sqlmodel import Session

from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.db import engine
from app.services.ingest import record_meter_reading, upsert_pstryk_prices
from app.services.timeseries import (
    hour_buckets,
    hourly_consumption_kwh,
    hourly_prices,
)


@pytest.fixture
def session():
    with Session(engine) as s:
        yield s


def _ts(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


def test_hour_buckets_is_half_open() -> None:
    buckets = hour_buckets(_ts(2026, 4, 27, 10, 30, 0), _ts(2026, 4, 27, 13, 0, 0))
    # Start is snapped down to top-of-hour; end is exclusive at the top of the hour.
    assert buckets == [
        _ts(2026, 4, 27, 10, 0, 0),
        _ts(2026, 4, 27, 11, 0, 0),
        _ts(2026, 4, 27, 12, 0, 0),
    ]


def test_hourly_consumption_buckets_intervals_by_midpoint(session: Session) -> None:
    # 60 s readings at 10:30, 10:45, 11:00, 11:15 — both intervals at 10:37
    # and 10:52 sit in hour 10; the 11:07 midpoint sits in hour 11.
    base = _ts(2026, 4, 27, 10, 30, 0)
    record_meter_reading(
        session, BleBoxReading(ts_utc=base, active_power_w=0.0, energy_kwh_total=10.0, raw={})
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(minutes=15),
            active_power_w=0.0,
            energy_kwh_total=10.25,
            raw={},
        ),
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(minutes=30),
            active_power_w=0.0,
            energy_kwh_total=10.5,
            raw={},
        ),
    )
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=base + timedelta(minutes=45),
            active_power_w=0.0,
            energy_kwh_total=10.8,
            raw={},
        ),
    )

    by_hour = hourly_consumption_kwh(
        session, _ts(2026, 4, 27, 10, 0, 0), _ts(2026, 4, 27, 12, 0, 0)
    )
    assert by_hour[_ts(2026, 4, 27, 10, 0, 0)] == pytest.approx(0.5)  # 0.25 + 0.25
    assert by_hour[_ts(2026, 4, 27, 11, 0, 0)] == pytest.approx(0.3)  # 10.8 - 10.5


def test_hourly_prices_returns_dict_keyed_by_ts(session: Session) -> None:
    upsert_pstryk_prices(
        session,
        [
            HourlyPrice(
                ts_utc=_ts(2026, 4, 27, 10, 0, 0), price_pln_per_kwh=0.42, kind="historical"
            ),
            HourlyPrice(
                ts_utc=_ts(2026, 4, 27, 11, 0, 0), price_pln_per_kwh=0.55, kind="historical"
            ),
            HourlyPrice(ts_utc=_ts(2026, 4, 27, 12, 0, 0), price_pln_per_kwh=0.65, kind="forecast"),
        ],
    )
    prices = hourly_prices(session, _ts(2026, 4, 27, 10, 0, 0), _ts(2026, 4, 27, 12, 0, 0))
    # End is exclusive — the 12:00 row is outside.
    assert sorted(prices.keys()) == [_ts(2026, 4, 27, 10, 0, 0), _ts(2026, 4, 27, 11, 0, 0)]
    assert prices[_ts(2026, 4, 27, 10, 0, 0)] == 0.42


def test_hourly_consumption_skips_intervals_without_kwh(session: Session) -> None:
    # Single reading: no pairs to integrate → empty result.
    record_meter_reading(
        session,
        BleBoxReading(
            ts_utc=_ts(2026, 4, 27, 10, 0, 0),
            active_power_w=100.0,
            energy_kwh_total=1.0,
            raw={},
        ),
    )
    assert (
        hourly_consumption_kwh(session, _ts(2026, 4, 27, 0, 0, 0), _ts(2026, 4, 28, 0, 0, 0)) == {}
    )
