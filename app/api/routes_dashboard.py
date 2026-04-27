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
from app.services.aggregations import period_totals
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


def _today_and_tomorrow_window_utc(tz: ZoneInfo):
    today = datetime.now(tz).date()
    start_local = datetime.combine(today, time.min).replace(tzinfo=tz)
    end_local = datetime.combine(today + timedelta(days=2), time.min).replace(tzinfo=tz)
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

    reading = state.last_reading
    current_price = latest_price_at(session, utcnow_naive())

    # Today / This month / This year all draw from Pstryk's stored
    # hourly meter values + cost (authoritative). The current hour,
    # which Pstryk hasn't reported yet, falls back to BleBox-derived
    # kWh × the stored hourly tariff.
    day_start, day_end = _local_day_window_utc(tz)
    today_totals = period_totals(session, day_start, day_end)
    month_start, month_end = _local_month_window_utc(tz)
    month_totals = period_totals(session, month_start, month_end)
    year_start, year_end = _local_year_window_utc(tz)
    year_totals = period_totals(session, year_start, year_end)

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
        "today_kwh": today_totals["kwh"],
        "today_cost_pln": today_totals["cost_pln"],
        "month_kwh": month_totals["kwh"],
        "month_cost_pln": month_totals["cost_pln"],
        "year_kwh": year_totals["kwh"],
        "year_cost_pln": year_totals["cost_pln"],
        "live_updated_at": reading.ts_utc if reading is not None else None,
    }
    return templates.TemplateResponse(request, "partials/live_tile.html", ctx)


@router.get("/partials/health-status", response_class=HTMLResponse)
def health_status(request: Request) -> HTMLResponse:
    """Render a banner if Pstryk or BleBox jobs reported errors recently.

    Returns an empty body when both feeds are healthy so the HTMX swap
    target collapses to nothing.
    """
    if state.pstryk_last_error is None and state.blebox_last_error is None:
        return HTMLResponse("")
    return templates.TemplateResponse(
        request,
        "partials/health_status.html",
        {
            "pstryk_error": state.pstryk_last_error,
            "blebox_error": state.blebox_last_error,
        },
    )


@router.get("/partials/backfill-status", response_class=HTMLResponse)
def backfill_status(request: Request) -> HTMLResponse:
    """Render the historical-data download banner.

    Returns:
    - empty body when status is idle/complete (HTMX swap collapses it)
    - a banner with progress when running
    - an error banner when failed
    """
    if state.backfill_status in ("idle", "complete"):
        return HTMLResponse("")
    return templates.TemplateResponse(
        request,
        "partials/backfill_status.html",
        {
            "status": state.backfill_status,
            "message": state.backfill_message,
            "chunks": state.backfill_chunks_done,
            "rows": state.backfill_rows_loaded,
        },
    )


@router.get("/partials/cheapest-hours", response_class=HTMLResponse)
def cheapest_hours(request: Request, session: SessionDep) -> HTMLResponse:
    """The cheapest / most-expensive remaining hours of today plus all
    of tomorrow. Tomorrow's prices are typically published by Pstryk
    around midday today; the card's tomorrow column fills in then.
    """
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    now_hour = utcnow_naive().replace(minute=0, second=0, microsecond=0)
    _, window_end = _today_and_tomorrow_window_utc(tz)

    rows = list(
        session.exec(
            select(PstrykPrice)
            .where(PstrykPrice.ts_utc >= now_hour)
            .where(PstrykPrice.ts_utc < window_end)
            .order_by(PstrykPrice.ts_utc)
        ).all()
    )

    today_local = datetime.now(tz).date()

    def to_local(dt: datetime) -> datetime:
        return dt.replace(tzinfo=ZoneInfo("UTC")).astimezone(tz)

    cheap_sorted = sorted(rows, key=lambda r: r.price_pln_per_kwh)[:8]
    cheap_sorted.sort(key=lambda r: r.ts_utc)
    exp_sorted = sorted(rows, key=lambda r: r.price_pln_per_kwh, reverse=True)[:5]
    exp_sorted.sort(key=lambda r: r.ts_utc)

    # Polish 3-letter weekday abbreviations, indexed by datetime.weekday()
    # (Monday=0). Hardcoded — avoids pulling in babel for a 7-element map.
    pl_weekday = ["pn", "wt", "śr", "czw", "pt", "sob", "nd"]

    def fmt(r: PstrykPrice) -> tuple[str, str, float, bool]:
        local = to_local(r.ts_utc)
        is_today = local.date() == today_local
        return (pl_weekday[local.weekday()], local.strftime("%H:%M"), r.price_pln_per_kwh, is_today)

    return templates.TemplateResponse(
        request,
        "partials/cheapest_hours.html",
        {
            "cheapest": [fmt(r) for r in cheap_sorted],
            "expensive": [fmt(r) for r in exp_sorted],
            "have_forecast": len(rows) > 0,
            "have_tomorrow": any(to_local(r.ts_utc).date() != today_local for r in rows),
        },
    )
