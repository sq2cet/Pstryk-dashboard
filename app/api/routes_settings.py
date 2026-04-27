from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlmodel import Session

from app.db import get_session
from app.services import settings_service as svc

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
    session: SessionDep,
    pstryk_api_key: Annotated[str, Form()] = "",
    blebox_host: Annotated[str, Form()] = "",
    blebox_port: Annotated[str, Form()] = "",
    pstryk_poll_minutes: Annotated[str, Form()] = "",
    blebox_live_seconds: Annotated[str, Form()] = "",
    blebox_persist_seconds: Annotated[str, Form()] = "",
) -> RedirectResponse:
    if pstryk_api_key.strip():
        svc.set_value(session, svc.PSTRYK_API_KEY, pstryk_api_key.strip())
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
    return RedirectResponse(url="/settings?saved=1", status_code=303)
