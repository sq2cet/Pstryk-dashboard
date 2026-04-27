from datetime import UTC, datetime

import pytest
from sqlmodel import Session, select

from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.db import engine
from app.models import MeterReading, PstrykPrice
from app.services.ingest import (
    latest_price_at,
    readings_in_range,
    record_meter_reading,
    upsert_pstryk_prices,
)


@pytest.fixture
def session():
    with Session(engine) as s:
        yield s


def _naive_utc(*args) -> datetime:
    return datetime(*args, tzinfo=UTC).replace(tzinfo=None)


def test_upsert_pstryk_prices_inserts_then_updates(session: Session) -> None:
    ts = _naive_utc(2026, 4, 27, 10, 0, 0)
    upsert_pstryk_prices(session, [HourlyPrice(ts_utc=ts, price_pln_per_kwh=0.42, kind="forecast")])
    upsert_pstryk_prices(
        session, [HourlyPrice(ts_utc=ts, price_pln_per_kwh=0.45, kind="historical")]
    )

    rows = session.exec(select(PstrykPrice)).all()
    assert len(rows) == 1
    assert rows[0].price_pln_per_kwh == 0.45
    assert rows[0].kind == "historical"


def test_record_meter_reading_avoids_pk_collision(session: Session) -> None:
    ts = _naive_utc(2026, 4, 27, 10, 0, 0)
    record_meter_reading(
        session, BleBoxReading(ts_utc=ts, active_power_w=100.0, energy_kwh_total=1.0, raw={})
    )
    record_meter_reading(
        session, BleBoxReading(ts_utc=ts, active_power_w=110.0, energy_kwh_total=1.05, raw={})
    )
    rows = session.exec(select(MeterReading).order_by(MeterReading.ts_utc)).all()
    assert len(rows) == 2
    assert rows[1].ts_utc.microsecond == 1


def test_record_meter_reading_normalises_none_power(session: Session) -> None:
    ts = _naive_utc(2026, 4, 27, 10, 0, 0)
    record_meter_reading(
        session, BleBoxReading(ts_utc=ts, active_power_w=None, energy_kwh_total=None, raw={})
    )
    row = session.exec(select(MeterReading)).first()
    assert row is not None
    assert row.active_power_w == 0.0
    assert row.energy_kwh_total is None


def test_latest_price_at_buckets_to_hour(session: Session) -> None:
    ts = _naive_utc(2026, 4, 27, 14, 0, 0)
    upsert_pstryk_prices(
        session,
        [HourlyPrice(ts_utc=ts, price_pln_per_kwh=0.50, kind="historical")],
    )
    found = latest_price_at(session, _naive_utc(2026, 4, 27, 14, 37, 22))
    assert found is not None
    assert found.price_pln_per_kwh == 0.50

    miss = latest_price_at(session, _naive_utc(2026, 4, 27, 15, 5, 0))
    assert miss is None


def test_readings_in_range_is_half_open(session: Session) -> None:
    for h in range(8, 12):
        record_meter_reading(
            session,
            BleBoxReading(
                ts_utc=_naive_utc(2026, 4, 27, h, 0, 0),
                active_power_w=100.0 * h,
                energy_kwh_total=float(h),
                raw={},
            ),
        )
    rows = readings_in_range(
        session, _naive_utc(2026, 4, 27, 9, 0, 0), _naive_utc(2026, 4, 27, 11, 0, 0)
    )
    hours = [r.ts_utc.hour for r in rows]
    assert hours == [9, 10]
