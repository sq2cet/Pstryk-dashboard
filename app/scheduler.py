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
    """At startup: ensure last 30 days + next 24 h of Pstryk metrics
    (price + kwh + cost) are stored. Idempotent — historical chunks
    that already have full kWh data are skipped, so a process restart
    doesn't burn API quota re-fetching what we already have. The
    forecast window is always fetched to refresh forward prices.
    """
    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured():
                return
            api_key = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
        if not api_key:
            return

        now = datetime.now(UTC)
        windows = [(now, now + timedelta(days=1))]
        for chunk_end_offset in range(0, 30, 7):
            end = now - timedelta(days=chunk_end_offset)
            windows.append((end - timedelta(days=7), end))

        async with PstrykClient(api_key=api_key) as client:
            for window_start, window_end in windows:
                if window_end <= now and _chunk_has_full_kwh(window_start, window_end):
                    logger.info(
                        "Backfill chunk %s..%s already hydrated, skipping",
                        window_start.date(),
                        window_end.date(),
                    )
                    continue
                try:
                    payload = await client.fetch_unified_metrics(window_start, window_end)
                except PstrykAPIError as exc:
                    logger.warning("Backfill chunk failed: %s", exc)
                    continue
                prices = parse_hourly_prices(payload)
                with Session(engine) as session:
                    upsert_pstryk_prices(session, prices)
                logger.info(
                    "Backfill chunk %s..%s: %d rows",
                    window_start.date(),
                    window_end.date(),
                    len(prices),
                )
    except Exception:
        logger.exception("Pstryk backfill crashed")


def _chunk_has_full_kwh(window_start: datetime, window_end: datetime) -> bool:
    """True if every historical row in the window already has both
    kwh_import AND cost_pln populated (Pstryk's authoritative meter +
    cost values). A NULL in either signals the chunk needs re-fetch.
    """
    start = window_start.replace(tzinfo=None) if window_start.tzinfo else window_start
    end = window_end.replace(tzinfo=None) if window_end.tzinfo else window_end
    with Session(engine) as session:
        missing = session.exec(
            select(PstrykPrice)
            .where(PstrykPrice.ts_utc >= start)
            .where(PstrykPrice.ts_utc < end)
            .where(
                (PstrykPrice.kwh_import.is_(None))  # type: ignore[union-attr]
                | (PstrykPrice.cost_pln.is_(None))  # type: ignore[union-attr]
            )
            .limit(1)
        ).first()
        if missing is not None:
            return False
        has_any = session.exec(
            select(PstrykPrice)
            .where(PstrykPrice.ts_utc >= start)
            .where(PstrykPrice.ts_utc < end)
            .limit(1)
        ).first()
        return has_any is not None


async def blebox_live_job() -> None:
    """Poll BleBox and refresh the in-memory live cache.

    Runs every `blebox_live_seconds` (default 5 s). The 60 s persist
    job reads the cached reading rather than re-hitting the device.
    """
    from app import state

    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured() or view.blebox_host is None:
                return
            host = view.blebox_host
            port = view.blebox_port

        async with BleBoxClient(host=host, port=port) as client:
            reading = await client.read_state()
        state.set_last_reading(reading)
    except BleBoxError as exc:
        logger.warning("BleBox live job error: %s", exc)
    except Exception:
        logger.exception("BleBox live job crashed")


def blebox_persist_job() -> None:
    """Persist the most recent cached BleBox reading to MeterReading."""
    from app import state

    try:
        reading = state.last_reading
        if reading is None:
            return
        with Session(engine) as session:
            record_meter_reading(session, reading)
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
        blebox_live_job,
        IntervalTrigger(seconds=view.blebox_live_seconds),
        id="blebox_live",
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
