import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlmodel import Session

from app.api import routes_charts, routes_dashboard, routes_settings
from app.db import get_session, init_db
from app.scheduler import build_scheduler, is_disabled
from app.services import settings_service as svc

logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=APP_DIR / "templates")
# Used as a cache-busting query param on static asset URLs so a new
# build automatically invalidates the browser's cached charts.js / app.css.
ASSET_VERSION = str(int(time.time()))
templates.env.globals["asset_version"] = ASSET_VERSION

SessionDep = Annotated[Session, Depends(get_session)]


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if is_disabled():
        logger.info("Scheduler disabled via PSTRYK_DISABLE_SCHEDULER=1")
        app.state.scheduler = None
    else:
        scheduler = build_scheduler()
        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))
    try:
        yield
    finally:
        if app.state.scheduler is not None:
            app.state.scheduler.shutdown(wait=False)


app = FastAPI(title="Pstryk Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")
app.include_router(routes_settings.router)
app.include_router(routes_dashboard.router)
app.include_router(routes_charts.router)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index(request: Request, session: SessionDep) -> Response:
    view = svc.get_view(session)
    if not view.is_configured():
        return RedirectResponse(url="/settings", status_code=303)
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {"title": "Pstryk Dashboard", "view": view},
    )
