"""Write Pstryk prices and BleBox meter readings into the database.

`upsert_pstryk_prices` is idempotent — re-running it for the same hour
overwrites the previous price rather than creating a duplicate. This
matches Pstryk's behaviour where a forecast hour eventually becomes a
historical hour with a slightly different gross value.
"""

from __future__ import annotations

import json

from sqlmodel import Session, select

from app.clients.blebox import BleBoxReading
from app.clients.pstryk import HourlyPrice
from app.models import MeterReading, PstrykPrice, utcnow_naive


def upsert_pstryk_prices(session: Session, prices: list[HourlyPrice]) -> int:
    """Upsert hourly prices + meter values + cost keyed by `ts_utc`.

    Pstryk's `meterValues` / `cost` are NULL on forecast rows, so we
    only overwrite the kwh/cost fields when a row carries them — a
    historical refresh must not blank-out a previously-stored kWh.
    """
    written = 0
    for p in prices:
        existing = session.get(PstrykPrice, p.ts_utc)
        if existing is None:
            session.add(
                PstrykPrice(
                    ts_utc=p.ts_utc,
                    price_pln_per_kwh=p.price_pln_per_kwh,
                    kind=p.kind,
                    raw_json=None,
                    fetched_at=utcnow_naive(),
                    kwh_import=p.kwh_import,
                    kwh_export=p.kwh_export,
                    cost_pln=p.cost_pln,
                )
            )
        else:
            existing.price_pln_per_kwh = p.price_pln_per_kwh
            existing.kind = p.kind
            existing.fetched_at = utcnow_naive()
            if p.kwh_import is not None:
                existing.kwh_import = p.kwh_import
            if p.kwh_export is not None:
                existing.kwh_export = p.kwh_export
            if p.cost_pln is not None:
                existing.cost_pln = p.cost_pln
            session.add(existing)
        written += 1
    session.commit()
    return written


def record_meter_reading(session: Session, reading: BleBoxReading) -> MeterReading:
    """Insert a meter reading. If an existing row shares the timestamp,
    bump the timestamp by 1 microsecond rather than fail — readings are
    high-cardinality and an exact duplicate is a benign clock collision.
    """
    ts = reading.ts_utc
    if session.get(MeterReading, ts) is not None:
        ts = ts.replace(microsecond=(ts.microsecond + 1) % 1_000_000)
    row = MeterReading(
        ts_utc=ts,
        active_power_w=reading.active_power_w if reading.active_power_w is not None else 0.0,
        energy_kwh_total=reading.energy_kwh_total,
        raw_json=json.dumps(reading.raw, separators=(",", ":")) if reading.raw else None,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def latest_price_at(session: Session, ts) -> PstrykPrice | None:
    """Return the price row whose hour bucket covers `ts` (UTC, naive)."""
    bucket = ts.replace(minute=0, second=0, microsecond=0)
    return session.get(PstrykPrice, bucket)


def readings_in_range(session: Session, start_utc, end_utc) -> list[MeterReading]:
    stmt = (
        select(MeterReading)
        .where(MeterReading.ts_utc >= start_utc)
        .where(MeterReading.ts_utc < end_utc)
        .order_by(MeterReading.ts_utc)
    )
    return list(session.exec(stmt).all())
