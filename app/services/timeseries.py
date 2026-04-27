"""Aggregate raw readings + prices into the time series the UI expects.

`hourly_consumption_kwh` buckets per-interval kWh into UTC hour buckets
by interval midpoint; the result is what the dashboard's combo chart
plots as bars. `hourly_prices` is a flat lookup of stored hourly prices
for the same window. The two share the same bucket key (a naive UTC
datetime at the top of the hour) so the UI can zip them.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlmodel import Session, select

from app.models import PstrykPrice
from app.services.cost import kwh_between
from app.services.ingest import readings_in_range


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
