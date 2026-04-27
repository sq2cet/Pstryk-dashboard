"""Translate raw meter readings + Pstryk hourly tariff into daily cost.

For each pair of consecutive readings inside a target day:

- The energy used in the interval is `b.energy_kwh_total - a.energy_kwh_total`
  when both readings expose the cumulative counter (preferred — robust
  against gaps). When the counter is missing or wraps backwards (device
  reboot), fall back to `avg_power_w * dt_hours / 1000`.
- The cost for the interval is `interval_kwh * price_at(midpoint)`.

Daily totals are summed across all intervals; the daily average price is
the energy-weighted mean (cost / kWh), which is what most users actually
care about, not the arithmetic mean of hourly prices.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from sqlmodel import Session

from app.models import DailyAggregate, MeterReading
from app.services.ingest import latest_price_at, readings_in_range


@dataclass(frozen=True)
class DailyTotals:
    day: date
    kwh: float
    cost_pln: float
    avg_price_pln_per_kwh: float


@dataclass(frozen=True)
class RangeTotals:
    kwh: float
    cost_pln: float
    avg_price_pln_per_kwh: float


def kwh_between(prev: MeterReading, curr: MeterReading) -> float | None:
    """Energy used between two consecutive readings, in kWh. None if undecidable."""
    if curr.ts_utc <= prev.ts_utc:
        return None
    if (
        prev.energy_kwh_total is not None
        and curr.energy_kwh_total is not None
        and curr.energy_kwh_total >= prev.energy_kwh_total
    ):
        return float(curr.energy_kwh_total - prev.energy_kwh_total)

    dt_hours = (curr.ts_utc - prev.ts_utc).total_seconds() / 3600.0
    if dt_hours <= 0:
        return None
    avg_w = (prev.active_power_w + curr.active_power_w) / 2.0
    return (avg_w * dt_hours) / 1000.0


def compute_day(session: Session, day: date, tz_name: str = "Europe/Warsaw") -> DailyTotals:
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(day, time.min).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)
    end_utc = end_local.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)

    readings = readings_in_range(session, start_utc, end_utc)
    if len(readings) < 2:
        return DailyTotals(day=day, kwh=0.0, cost_pln=0.0, avg_price_pln_per_kwh=0.0)

    total_kwh = 0.0
    total_cost = 0.0
    for prev, curr in zip(readings, readings[1:], strict=False):
        kwh = kwh_between(prev, curr)
        if kwh is None or kwh <= 0:
            continue
        midpoint = prev.ts_utc + (curr.ts_utc - prev.ts_utc) / 2
        price = latest_price_at(session, midpoint)
        if price is None:
            continue
        total_kwh += kwh
        total_cost += kwh * price.price_pln_per_kwh

    avg_price = (total_cost / total_kwh) if total_kwh > 0 else 0.0
    return DailyTotals(day=day, kwh=total_kwh, cost_pln=total_cost, avg_price_pln_per_kwh=avg_price)


def compute_range(session: Session, start_utc: datetime, end_utc: datetime) -> RangeTotals:
    """Sum consumption + cost across an arbitrary UTC range.

    Used for "this month" / "this year" tiles where bucketing per day
    would be wasteful — the running total is what matters.
    """
    readings = readings_in_range(session, start_utc, end_utc)
    if len(readings) < 2:
        return RangeTotals(kwh=0.0, cost_pln=0.0, avg_price_pln_per_kwh=0.0)

    total_kwh = 0.0
    total_cost = 0.0
    for prev, curr in zip(readings, readings[1:], strict=False):
        kwh = kwh_between(prev, curr)
        if kwh is None or kwh <= 0:
            continue
        midpoint = prev.ts_utc + (curr.ts_utc - prev.ts_utc) / 2
        price = latest_price_at(session, midpoint)
        if price is None:
            continue
        total_kwh += kwh
        total_cost += kwh * price.price_pln_per_kwh

    avg_price = (total_cost / total_kwh) if total_kwh > 0 else 0.0
    return RangeTotals(kwh=total_kwh, cost_pln=total_cost, avg_price_pln_per_kwh=avg_price)


def materialise_day(session: Session, day: date, tz_name: str = "Europe/Warsaw") -> DailyAggregate:
    """Compute the daily total and upsert into `DailyAggregate`."""
    totals = compute_day(session, day, tz_name)
    row = session.get(DailyAggregate, day)
    if row is None:
        row = DailyAggregate(
            day=day,
            kwh=totals.kwh,
            avg_price_pln_per_kwh=totals.avg_price_pln_per_kwh,
            cost_pln=totals.cost_pln,
        )
        session.add(row)
    else:
        row.kwh = totals.kwh
        row.avg_price_pln_per_kwh = totals.avg_price_pln_per_kwh
        row.cost_pln = totals.cost_pln
        session.add(row)
    session.commit()
    session.refresh(row)
    return row
