"""JSON endpoints feeding Chart.js on the dashboard."""

from __future__ import annotations

from dataclasses import asdict
from datetime import timedelta
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlmodel import Session

from app import state
from app.db import get_session
from app.models import utcnow_naive
from app.services import settings_service as svc
from app.services.aggregations import aggregate_range, resolve_window
from app.services.timeseries import (
    hour_buckets,
    hourly_consumption_kwh,
    hourly_prices,
)

router = APIRouter()
SessionDep = Annotated[Session, Depends(get_session)]


@router.get("/api/charts/hourly")
def hourly_chart(
    session: SessionDep,
    hours: Annotated[int, Query(ge=1, le=720)] = 24,
    forecast_hours: Annotated[int, Query(ge=0, le=48)] = 24,
) -> dict:
    """Return hour-by-hour price + consumption series.

    The window starts `hours` ago in UTC and extends `forecast_hours`
    into the future. Each bucket carries:
      - `bucket_utc`: ISO timestamp of the hour start (UTC)
      - `bucket_local`: ISO timestamp in the user's timezone
      - `price_pln_per_kwh`: from `PstrykPrice`, may be null
      - `kwh`: consumption attributed to this hour, may be null
      - `is_now`: true for the bucket containing the current hour
    """
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    current_hour = utcnow_naive().replace(minute=0, second=0, microsecond=0)
    # `hours` counts buckets back from now *including* the current hour;
    # `forecast_hours` counts buckets after now. The end of the window
    # is exclusive so we add one to land just past the last bucket.
    start_utc = current_hour - timedelta(hours=hours - 1)
    end_utc = current_hour + timedelta(hours=forecast_hours + 1)

    prices = hourly_prices(session, start_utc, end_utc)
    consumption = hourly_consumption_kwh(session, start_utc, end_utc)
    buckets = hour_buckets(start_utc, end_utc)

    series = []
    for b in buckets:
        b_local = b.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)
        series.append(
            {
                "bucket_utc": b.isoformat() + "Z",
                "bucket_local": b_local.isoformat(),
                "price_pln_per_kwh": prices.get(b),
                "kwh": consumption.get(b),
                "is_now": b == current_hour,
            }
        )
    return {
        "tz": view.tz,
        "history_hours": hours,
        "forecast_hours": forecast_hours,
        "series": series,
    }


@router.get("/api/charts/live-power")
def live_power(
    session: SessionDep,
    minutes: Annotated[int, Query(ge=1, le=120)] = 60,
) -> dict:
    """Return the in-memory rolling buffer of recent BleBox readings.

    Each point carries total active power and a per-phase breakdown.
    Used by the live-power chart, which polls this endpoint every 5 s.
    """
    view = svc.get_view(session)
    cutoff = utcnow_naive() - timedelta(minutes=minutes)
    points = [r for r in state.recent_readings if r.ts_utc >= cutoff]

    def phase_w(r, attr: str) -> float | None:
        phase = getattr(r, attr, None)
        return phase.active_power_w if phase is not None else None

    return {
        "tz": view.tz,
        "minutes": minutes,
        "ts": [r.ts_utc.isoformat() + "Z" for r in points],
        "total_w": [r.active_power_w for r in points],
        "l1_w": [phase_w(r, "phase_l1") for r in points],
        "l2_w": [phase_w(r, "phase_l2") for r in points],
        "l3_w": [phase_w(r, "phase_l3") for r in points],
    }


@router.get("/api/charts/range")
def range_chart(
    session: SessionDep,
    range_: Annotated[str, Query(alias="range")] = "today",
    resolution: Annotated[str | None, Query()] = None,
    from_: Annotated[str | None, Query(alias="from")] = None,
    to_: Annotated[str | None, Query(alias="to")] = None,
) -> dict:
    """Aggregate metrics over a range at a chosen resolution.

    `range` is one of {24h, today, week, month, year, custom}.
    `resolution` is one of {hour, day, month, year}; defaults to a
    sensible value for the chosen range.
    `from`/`to` are required when `range=custom` and are ISO local
    dates (YYYY-MM-DD).
    """
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    try:
        start_utc, end_utc, default_resolution = resolve_window(range_, tz, from_, to_)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    res = resolution or default_resolution
    if res not in {"hour", "day", "month", "year"}:
        raise HTTPException(status_code=400, detail=f"unknown resolution: {res}")

    buckets, totals, cumulative_cost, cumulative_kwh = aggregate_range(
        session, start_utc, end_utc, res, view.tz
    )

    return {
        "tz": view.tz,
        "range": range_,
        "resolution": res,
        "start_utc": start_utc.isoformat() + "Z",
        "end_utc": end_utc.isoformat() + "Z",
        "buckets": [asdict(b) | {"bucket_utc": b.bucket_utc.isoformat() + "Z"} for b in buckets],
        "totals": totals,
        "cumulative_cost_pln": cumulative_cost,
        "cumulative_kwh": cumulative_kwh,
    }
