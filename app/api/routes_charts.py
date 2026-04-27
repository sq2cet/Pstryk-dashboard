"""JSON endpoints feeding Chart.js on the dashboard."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Query
from sqlmodel import Session

from app.db import get_session
from app.models import utcnow_naive
from app.services import settings_service as svc
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
