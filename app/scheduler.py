"""Background polling and aggregation jobs.

A single AsyncIOScheduler is started in FastAPI's lifespan. Jobs read
settings from the DB on every tick so a settings change becomes visible
on the next run without restarting (cadence/interval changes still need
a restart — APScheduler bakes the trigger interval at job creation).

Jobs swallow their own exceptions and log; one failure must not kill the
scheduler. Each job opens its own DB session.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlmodel import Session, select

from app.clients.blebox import BleBoxClient, BleBoxError
from app.clients.pstryk import (
    PstrykAPIError,
    PstrykClient,
    parse_hourly_prices,
)
from app.db import engine
from app.models import PstrykPrice
from app.services import settings_service as svc
from app.services.cost import materialise_day
from app.services.ingest import record_meter_reading, upsert_pstryk_prices

logger = logging.getLogger(__name__)


def is_disabled() -> bool:
    return os.environ.get("PSTRYK_DISABLE_SCHEDULER") == "1"


async def pstryk_poll_job() -> None:
    """Pull the last 25h + next 24h of hourly prices, upsert."""
    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured():
                return
            api_key = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
        if not api_key:
            return

        now = datetime.now(UTC)
        window_start = now - timedelta(hours=25)
        window_end = now + timedelta(hours=24)

        async with PstrykClient(api_key=api_key) as client:
            payload = await client.fetch_unified_metrics(window_start, window_end)
        prices = parse_hourly_prices(payload)
        if not prices:
            logger.warning("Pstryk poll returned no parseable prices")
            return

        with Session(engine) as session:
            written = upsert_pstryk_prices(session, prices)
        logger.info("Pstryk poll: wrote %d hourly prices", written)
    except PstrykAPIError as exc:
        logger.warning("Pstryk poll API error: %s", exc)
    except Exception:
        logger.exception("Pstryk poll job crashed")


async def pstryk_backfill_30d_job() -> None:
    """One-shot: if the price table is empty, pull the last 30 days."""
    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured():
                return
            existing = session.exec(select(PstrykPrice).limit(1)).first()
            if existing is not None:
                return
            api_key = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
        if not api_key:
            return

        now = datetime.now(UTC)
        async with PstrykClient(api_key=api_key) as client:
            # Fetch in 7-day chunks to keep per-call response sizes reasonable
            # while staying within the 3-req/hour rate limit on this endpoint.
            for chunk_end_offset in range(0, 30, 7):
                window_end = now - timedelta(days=chunk_end_offset)
                window_start = window_end - timedelta(days=7)
                try:
                    payload = await client.fetch_unified_metrics(window_start, window_end)
                except PstrykAPIError as exc:
                    logger.warning("Backfill chunk failed: %s", exc)
                    continue
                prices = parse_hourly_prices(payload)
                with Session(engine) as session:
                    upsert_pstryk_prices(session, prices)
                logger.info(
                    "Backfill chunk %s..%s: %d prices",
                    window_start.date(),
                    window_end.date(),
                    len(prices),
                )
    except Exception:
        logger.exception("Pstryk backfill crashed")


async def blebox_persist_job() -> None:
    """Read the BleBox state and persist a single MeterReading row."""
    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured() or view.blebox_host is None:
                return
            host = view.blebox_host
            port = view.blebox_port

        async with BleBoxClient(host=host, port=port) as client:
            reading = await client.read_state()

        with Session(engine) as session:
            record_meter_reading(session, reading)
    except BleBoxError as exc:
        logger.warning("BleBox persist error: %s", exc)
    except Exception:
        logger.exception("BleBox persist job crashed")


def daily_aggregate_job() -> None:
    """Recompute yesterday + today's `DailyAggregate` rows."""
    try:
        from zoneinfo import ZoneInfo

        with Session(engine) as session:
            view = svc.get_view(session)
            tz_name = view.tz
            tz = ZoneInfo(tz_name)
            today_local = datetime.now(tz).date()
            yesterday_local = today_local - timedelta(days=1)
            materialise_day(session, yesterday_local, tz_name)
            materialise_day(session, today_local, tz_name)
        logger.info("Daily aggregate: recomputed %s and %s", yesterday_local, today_local)
    except Exception:
        logger.exception("Daily aggregate job crashed")


def build_scheduler() -> AsyncIOScheduler:
    """Create and configure the scheduler with the current settings."""
    sched = AsyncIOScheduler(timezone=UTC)

    with Session(engine) as session:
        view = svc.get_view(session)

    sched.add_job(
        pstryk_poll_job,
        IntervalTrigger(minutes=view.pstryk_poll_minutes),
        id="pstryk_poll",
        max_instances=1,
        coalesce=True,
    )
    sched.add_job(
        blebox_persist_job,
        IntervalTrigger(seconds=view.blebox_persist_seconds),
        id="blebox_persist",
        max_instances=1,
        coalesce=True,
    )
    sched.add_job(
        daily_aggregate_job,
        CronTrigger(hour=2, minute=0, timezone=view.tz),
        id="daily_aggregate",
        max_instances=1,
        coalesce=True,
    )
    sched.add_job(
        pstryk_backfill_30d_job,
        next_run_time=datetime.now(UTC) + timedelta(seconds=10),
        id="pstryk_backfill",
        max_instances=1,
    )
    return sched
