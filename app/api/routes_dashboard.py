"""HTML fragments served to HTMX targets on the dashboard."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Annotated
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session

from app import state
from app.db import get_session
from app.models import utcnow_naive
from app.services import settings_service as svc
from app.services.cost import compute_day
from app.services.ingest import latest_price_at

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)
router = APIRouter()

SessionDep = Annotated[Session, Depends(get_session)]


@router.get("/partials/live", response_class=HTMLResponse)
def live_tile(request: Request, session: SessionDep) -> HTMLResponse:
    view = svc.get_view(session)
    tz = ZoneInfo(view.tz)
    today_local = datetime.now(tz).date()

    reading = state.last_reading
    current_price = latest_price_at(session, utcnow_naive())
    today_totals = compute_day(session, today_local, tz_name=view.tz)

    ctx = {
        "active_power_w": reading.active_power_w if reading is not None else None,
        "current_price_pln_per_kwh": (
            current_price.price_pln_per_kwh if current_price is not None else None
        ),
        "today_kwh": today_totals.kwh,
        "today_cost_pln": today_totals.cost_pln,
        "live_updated_at": reading.ts_utc if reading is not None else None,
    }
    return templates.TemplateResponse(request, "partials/live_tile.html", ctx)
