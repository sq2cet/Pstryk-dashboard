"""Aggregate raw readings + prices into the time series the UI expects.

For each UTC hour we may have three data sources:

- Pstryk's stored hourly meter reading (`PstrykPrice.kwh_import`) —
  authoritative when the hour is in the past and the API has reported
  consumption for it.
- Pstryk's stored hourly cost (`PstrykPrice.cost_pln`) — same.
- A trail of BleBox `MeterReading` rows we wrote ourselves at 60 s
  cadence. The current hour and any hour before Pstryk has reported
  fall back to this.

`hourly_metrics` merges all three and returns one record per hour
exposing `kwh`, `cost_pln`, and `price_pln_per_kwh`.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlmodel import Session, select

from app.models import PstrykPrice
from app.services.cost import kwh_between
from app.services.ingest import readings_in_range


@dataclass(frozen=True)
class HourlyMetric:
    kwh: float | None
    cost_pln: float | None
    price_pln_per_kwh: float | None


def hourly_consumption_kwh(
    session: Session, start_utc: datetime, end_utc: datetime
) -> dict[datetime, float]:
    """Return {hour_bucket_utc: kwh} for the given UTC range.

    Buckets are keyed on the start of the UTC hour. Intervals are
    attributed to the hour their midpoint falls in (matches the cost
    service's accounting).
    """
    readings = readings_in_range(session, start_utc, end_utc)
    by_hour: dict[datetime, float] = {}
    for prev, curr in zip(readings, readings[1:], strict=False):
        kwh = kwh_between(prev, curr)
        if kwh is None or kwh <= 0:
            continue
        midpoint = prev.ts_utc + (curr.ts_utc - prev.ts_utc) / 2
        bucket = midpoint.replace(minute=0, second=0, microsecond=0)
        by_hour[bucket] = by_hour.get(bucket, 0.0) + kwh
    return by_hour


def hourly_prices(
    session: Session, start_utc: datetime, end_utc: datetime
) -> dict[datetime, float]:
    """Return {hour_bucket_utc: price_pln_per_kwh} for the given UTC range."""
    stmt = (
        select(PstrykPrice)
        .where(PstrykPrice.ts_utc >= start_utc)
        .where(PstrykPrice.ts_utc < end_utc)
        .order_by(PstrykPrice.ts_utc)
    )
    rows = session.exec(stmt).all()
    return {row.ts_utc: row.price_pln_per_kwh for row in rows}


def hourly_metrics(
    session: Session, start_utc: datetime, end_utc: datetime
) -> dict[datetime, HourlyMetric]:
    """Per-hour merged metrics over a UTC range.

    Pstryk's stored kwh + cost win; BleBox-derived kwh fills the gaps
    (typically the current hour or hours before Pstryk reports). When
    only price + kwh are known, cost is derived as kwh * price.
    """
    bb_kwh = hourly_consumption_kwh(session, start_utc, end_utc)

    rows = session.exec(
        select(PstrykPrice)
        .where(PstrykPrice.ts_utc >= start_utc)
        .where(PstrykPrice.ts_utc < end_utc)
    ).all()
    by_ts: dict[datetime, PstrykPrice] = {r.ts_utc: r for r in rows}

    keys = set(by_ts) | set(bb_kwh)
    out: dict[datetime, HourlyMetric] = {}
    for ts in keys:
        row = by_ts.get(ts)
        price = row.price_pln_per_kwh if row else None
        kwh = (row.kwh_import if row and row.kwh_import is not None else None) or bb_kwh.get(ts)
        cost = row.cost_pln if row and row.cost_pln is not None else None
        if cost is None and kwh is not None and price is not None:
            cost = kwh * price
        out[ts] = HourlyMetric(kwh=kwh, cost_pln=cost, price_pln_per_kwh=price)
    return out


def hour_buckets(start_utc: datetime, end_utc: datetime) -> list[datetime]:
    """Inclusive list of hour bucket starts spanning [start_utc, end_utc)."""
    start = start_utc.replace(minute=0, second=0, microsecond=0)
    end = end_utc.replace(minute=0, second=0, microsecond=0)
    out: list[datetime] = []
    cur = start
    while cur < end:
        out.append(cur)
        cur += timedelta(hours=1)
    return out
