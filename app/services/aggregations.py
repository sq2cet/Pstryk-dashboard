"""Bucket hourly readings + prices into hour/day/month/year aggregates.

`aggregate_range` is the single entry point used by the chart endpoint
and the aggregates table. It always pulls hourly source data from the
DB (the finest grain we have), then groups into bigger buckets in the
user's timezone for day/month/year resolutions.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from sqlmodel import Session

from app.services.timeseries import hour_buckets, hourly_consumption_kwh, hourly_prices

Resolution = Literal["hour", "day", "month", "year"]
RangePreset = Literal["24h", "today", "week", "month", "year", "custom"]


@dataclass(frozen=True)
class BucketRow:
    bucket_utc: datetime
    bucket_local_iso: str
    kwh: float | None
    cost_pln: float | None
    avg_price_pln_per_kwh: float | None
    min_price_pln_per_kwh: float | None
    max_price_pln_per_kwh: float | None
    is_now: bool


@dataclass(frozen=True)
class RangeResult:
    range_label: str
    resolution: Resolution
    tz_name: str
    start_utc: datetime
    end_utc: datetime
    buckets: list[BucketRow]
    totals: dict
    cumulative_cost_pln: list[float | None]  # one per bucket, running total


def _to_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


def resolve_window(
    range_: RangePreset,
    tz: ZoneInfo,
    from_: str | None = None,
    to_: str | None = None,
) -> tuple[datetime, datetime, Resolution]:
    """Return (start_utc, end_utc, default_resolution) for a preset."""
    now_local = datetime.now(tz)
    today_local = now_local.date()

    if range_ == "24h":
        end_utc = (
            datetime.now(UTC).replace(tzinfo=None).replace(minute=0, second=0, microsecond=0)
        ) + timedelta(hours=1)
        start_utc = end_utc - timedelta(hours=24)
        return start_utc, end_utc, "hour"

    if range_ == "today":
        start_local = datetime.combine(today_local, time.min).replace(tzinfo=tz)
        return _to_utc_naive(start_local), _to_utc_naive(start_local + timedelta(days=1)), "hour"

    if range_ == "week":
        start_local = datetime.combine(today_local - timedelta(days=6), time.min).replace(tzinfo=tz)
        end_local = datetime.combine(today_local + timedelta(days=1), time.min).replace(tzinfo=tz)
        return _to_utc_naive(start_local), _to_utc_naive(end_local), "day"

    if range_ == "month":
        start_local = datetime.combine(today_local.replace(day=1), time.min).replace(tzinfo=tz)
        if start_local.month == 12:
            end_local = start_local.replace(year=start_local.year + 1, month=1)
        else:
            end_local = start_local.replace(month=start_local.month + 1)
        return _to_utc_naive(start_local), _to_utc_naive(end_local), "day"

    if range_ == "year":
        start_local = datetime.combine(today_local.replace(month=1, day=1), time.min).replace(
            tzinfo=tz
        )
        end_local = start_local.replace(year=start_local.year + 1)
        return _to_utc_naive(start_local), _to_utc_naive(end_local), "month"

    if range_ == "custom":
        if not from_ or not to_:
            raise ValueError("custom range requires from and to")
        f_d = date.fromisoformat(from_)
        t_d = date.fromisoformat(to_)
        start_local = datetime.combine(f_d, time.min).replace(tzinfo=tz)
        end_local = datetime.combine(t_d + timedelta(days=1), time.min).replace(tzinfo=tz)
        return _to_utc_naive(start_local), _to_utc_naive(end_local), "day"

    raise ValueError(f"unknown range preset: {range_}")


def _bucket_key_for(b_utc: datetime, resolution: Resolution, tz: ZoneInfo) -> datetime:
    """Map an hour bucket to its parent bucket for the chosen resolution.

    Returned key is a tz-aware local datetime at the start of the bucket.
    """
    local = b_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
    if resolution == "hour":
        return local
    if resolution == "day":
        return local.replace(hour=0, minute=0, second=0, microsecond=0)
    if resolution == "month":
        return local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if resolution == "year":
        return local.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    raise ValueError(f"unknown resolution: {resolution}")


def aggregate_range(
    session: Session,
    start_utc: datetime,
    end_utc: datetime,
    resolution: Resolution,
    tz_name: str,
) -> tuple[list[BucketRow], dict, list[float | None]]:
    """Return (buckets, totals, cumulative_cost) for the given window."""
    tz = ZoneInfo(tz_name)
    prices_h = hourly_prices(session, start_utc, end_utc)
    kwh_h = hourly_consumption_kwh(session, start_utc, end_utc)
    h_buckets = hour_buckets(start_utc, end_utc)

    # Group hour-level metrics by resolution-level bucket key.
    grouped: dict[datetime, dict] = {}
    for hb in h_buckets:
        key = _bucket_key_for(hb, resolution, tz)
        bucket = grouped.setdefault(
            key,
            {"prices": [], "kwh": [], "weighted_cost": 0.0, "weighted_kwh": 0.0},
        )
        p = prices_h.get(hb)
        k = kwh_h.get(hb)
        if p is not None:
            bucket["prices"].append(p)
        if k is not None:
            bucket["kwh"].append(k)
            if p is not None:
                bucket["weighted_cost"] += p * k
                bucket["weighted_kwh"] += k

    # Now-marker
    current_hour_utc = (
        datetime.now(UTC).replace(tzinfo=None).replace(minute=0, second=0, microsecond=0)
    )
    current_key = _bucket_key_for(current_hour_utc, resolution, tz)

    rows: list[BucketRow] = []
    cumulative: list[float | None] = []
    running_cost = 0.0
    total_kwh = 0.0
    total_cost = 0.0
    all_prices: list[float] = []

    for key in sorted(grouped):
        b = grouped[key]
        kwh_sum = sum(b["kwh"]) if b["kwh"] else None
        if b["weighted_kwh"] > 0:
            avg_price = b["weighted_cost"] / b["weighted_kwh"]
            cost = b["weighted_cost"]
        else:
            avg_price = (sum(b["prices"]) / len(b["prices"])) if b["prices"] else None
            cost = None
        min_p = min(b["prices"]) if b["prices"] else None
        max_p = max(b["prices"]) if b["prices"] else None

        if cost is not None:
            running_cost += cost
            cumulative.append(running_cost)
            total_cost += cost
        else:
            cumulative.append(running_cost if rows else None)

        if kwh_sum is not None:
            total_kwh += kwh_sum
        if b["prices"]:
            all_prices.extend(b["prices"])

        rows.append(
            BucketRow(
                bucket_utc=_to_utc_naive(key),
                bucket_local_iso=key.isoformat(),
                kwh=kwh_sum,
                cost_pln=cost,
                avg_price_pln_per_kwh=avg_price,
                min_price_pln_per_kwh=min_p,
                max_price_pln_per_kwh=max_p,
                is_now=(key == current_key),
            )
        )

    totals = {
        "kwh": total_kwh,
        "cost_pln": total_cost,
        "avg_price_pln_per_kwh": (
            (total_cost / total_kwh)
            if total_kwh > 0
            else (sum(all_prices) / len(all_prices) if all_prices else None)
        ),
        "min_price_pln_per_kwh": min(all_prices) if all_prices else None,
        "max_price_pln_per_kwh": max(all_prices) if all_prices else None,
        "bucket_count": len(rows),
    }
    return rows, totals, cumulative
