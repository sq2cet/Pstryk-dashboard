import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session, delete

from app import state
from app.db import get_session
from app.models import PstrykPrice
from app.services import settings_service as svc

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=TEMPLATES_DIR)

router = APIRouter()

SessionDep = Annotated[Session, Depends(get_session)]


@router.get("/settings", response_class=HTMLResponse)
def get_settings(
    request: Request,
    session: SessionDep,
    saved: bool = False,
) -> HTMLResponse:
    view = svc.get_view(session)
    return templates.TemplateResponse(
        request,
        "settings.html",
        {"title": "Settings — Pstryk Dashboard", "view": view, "saved": saved},
    )


@router.post("/settings")
def post_settings(
    request: Request,
    session: SessionDep,
    pstryk_api_key: Annotated[str, Form()] = "",
    blebox_host: Annotated[str, Form()] = "",
    blebox_port: Annotated[str, Form()] = "",
    pstryk_poll_minutes: Annotated[str, Form()] = "",
    blebox_live_seconds: Annotated[str, Form()] = "",
    blebox_persist_seconds: Annotated[str, Form()] = "",
) -> RedirectResponse:
    pstryk_key_changed = False
    if pstryk_api_key.strip():
        new_key = pstryk_api_key.strip()
        existing_key = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
        if existing_key != new_key:
            pstryk_key_changed = True
        svc.set_value(session, svc.PSTRYK_API_KEY, new_key)
    if blebox_host.strip():
        svc.set_value(session, svc.BLEBOX_HOST, blebox_host.strip())
    for key, raw in (
        (svc.BLEBOX_PORT, blebox_port),
        (svc.PSTRYK_POLL_MINUTES, pstryk_poll_minutes),
        (svc.BLEBOX_LIVE_SECONDS, blebox_live_seconds),
        (svc.BLEBOX_PERSIST_SECONDS, blebox_persist_seconds),
    ):
        if raw.strip().isdigit():
            svc.set_value(session, key, raw.strip())

    if pstryk_key_changed:
        # New API key may map to a different account / contract; clear
        # the local price + meter cache and trigger a fresh backfill so
        # the dashboard never shows the previous account's history.
        deleted = session.exec(delete(PstrykPrice)).rowcount  # type: ignore[attr-defined]
        session.commit()
        state.set_pstryk_error(None)
        state.backfill_start()
        logger.info(
            "Pstryk API key changed: cleared %s historical rows, scheduling backfill",
            deleted,
        )
        scheduler = getattr(request.app.state, "scheduler", None)
        if scheduler is not None:
            from app.scheduler import pstryk_backfill_all_job

            scheduler.add_job(
                pstryk_backfill_all_job,
                next_run_time=datetime.now(UTC) + timedelta(seconds=2),
                id="pstryk_backfill_manual",
                replace_existing=True,
                max_instances=1,
            )

    return RedirectResponse(url="/settings?saved=1", status_code=303)
