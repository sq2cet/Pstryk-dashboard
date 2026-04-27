"""HTML fragments served to HTMX targets on the dashboard."""

from __future__ import annotations

from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, select

from app import state
from app.db import get_session
from app.models import PstrykPrice, utcnow_naive
from app.services import settings_service as svc
from app.services.cost import compute_day, compute_range
from app.services.ingest import latest_price_at

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)
router = APIRouter()

SessionDep = Annotated[Session, Depends(get_session)]


def _local_day_window_utc(tz: ZoneInfo, days_back: int = 0):
    today = datetime.now(tz).date() - timedelta(days=days_back)
    start_local = datetime.combine(today, time.min).replace(tzinfo=tz)
    end_local = start_local + timedelta(days=1)
    return _to_utc_naive(start_local), _to_utc_naive(end_local)


def _local_month_window_utc(tz: ZoneInfo):
    today = datetime.now(tz).date()
    start_local = datetime.combine(today.replace(day=1), time.min).replace(tzinfo=tz)
    if start_local.month == 12:
        next_month_local = start_local.replace(year=start_local.year + 1, month=1)
    else:
        next_month_local = start_local.replace(month=start_local.month + 1)
    return _to_utc_naive(start_local), _to_utc_naive(next_month_local)


def _local_year_window_utc(tz: ZoneInfo):
    today = datetime.now(tz).date()
    start_local = datetime.combine(today.replace(month=1, day=1), time.min).replace(tzinfo=tz)
    next_year_local = start_local.replace(year=start_local.year + 1)
    return _to_utc_naive(start_local), _to_utc_naive(next_year_local)


def _to_utc_naive(dt: datetime) -> datetime:
    return dt.astimezone(ZoneInfo("UTC")).replace(tzinfo=None)


@router.get("/partials/live", response_class=HTMLResponse)
def live_tile(request: Request, session: SessionDep) -> HTMLResponse:
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    today_local = datetime.now(tz).date()

    reading = state.last_reading
    current_price = latest_price_at(session, utcnow_naive())

    today_totals = compute_day(session, today_local, tz_name=view.tz)
    month_start, month_end = _local_month_window_utc(tz)
    month_totals = compute_range(session, month_start, month_end)
    year_start, year_end = _local_year_window_utc(tz)
    year_totals = compute_range(session, year_start, year_end)

    is_cheap_now = False
    is_expensive_now = False
    if current_price and current_price.raw_json:
        # The raw payload would expose is_cheap/is_expensive; we did not
        # persist these flags, so use a simple heuristic: compute the
        # last 24h cheap/expensive thresholds from stored prices.
        pass
    if current_price is not None:
        last_24h_cutoff = utcnow_naive() - timedelta(hours=24)
        prices_24h = session.exec(
            select(PstrykPrice).where(PstrykPrice.ts_utc >= last_24h_cutoff)
        ).all()
        prices_only = [p.price_pln_per_kwh for p in prices_24h]
        if len(prices_only) >= 6:
            sorted_prices = sorted(prices_only)
            cheap_threshold = sorted_prices[len(sorted_prices) // 4]  # bottom quartile
            expensive_threshold = sorted_prices[3 * len(sorted_prices) // 4]  # top quartile
            is_cheap_now = current_price.price_pln_per_kwh <= cheap_threshold
            is_expensive_now = current_price.price_pln_per_kwh >= expensive_threshold

    ctx = {
        "reading": reading,
        "current_price_pln_per_kwh": (
            current_price.price_pln_per_kwh if current_price is not None else None
        ),
        "is_cheap_now": is_cheap_now,
        "is_expensive_now": is_expensive_now,
        "today_kwh": today_totals.kwh,
        "today_cost_pln": today_totals.cost_pln,
        "month_kwh": month_totals.kwh,
        "month_cost_pln": month_totals.cost_pln,
        "year_kwh": year_totals.kwh,
        "year_cost_pln": year_totals.cost_pln,
        "live_updated_at": reading.ts_utc if reading is not None else None,
    }
    return templates.TemplateResponse(request, "partials/live_tile.html", ctx)


@router.get("/partials/cheapest-hours", response_class=HTMLResponse)
def cheapest_hours(request: Request, session: SessionDep) -> HTMLResponse:
    """The cheapest forecast hours in the next 24 h."""
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    now = utcnow_naive()
    end = now + timedelta(hours=24)

    rows = session.exec(
        select(PstrykPrice)
        .where(PstrykPrice.ts_utc >= now.replace(minute=0, second=0, microsecond=0))
        .where(PstrykPrice.ts_utc < end)
        .order_by(PstrykPrice.price_pln_per_kwh)
    ).all()

    cheapest = list(rows[:5])
    cheapest.sort(key=lambda r: r.ts_utc)
    expensive = sorted(rows, key=lambda r: r.price_pln_per_kwh, reverse=True)[:3]
    expensive.sort(key=lambda r: r.ts_utc)

    def to_local_str(dt: datetime) -> str:
        return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz).strftime("%a %H:%M")

    return templates.TemplateResponse(
        request,
        "partials/cheapest_hours.html",
        {
            "cheapest": [(to_local_str(r.ts_utc), r.price_pln_per_kwh) for r in cheapest],
            "expensive": [(to_local_str(r.ts_utc), r.price_pln_per_kwh) for r in expensive],
            "have_forecast": len(rows) > 0,
        },
    )
