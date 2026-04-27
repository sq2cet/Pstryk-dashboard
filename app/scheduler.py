"""Background polling and aggregation jobs.

A single AsyncIOScheduler is started in FastAPI's lifespan. Jobs read
settings from the DB on every tick so a settings change becomes visible
on the next run without restarting (cadence/interval changes still need
a restart — APScheduler bakes the trigger interval at job creation).

Jobs swallow their own exceptions and log; one failure must not kill the
scheduler. Each job opens its own DB session.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from sqlmodel import Session, select

from app import state
from app.clients.blebox import (
    BleBoxClient,
    BleBoxError,
    UnsupportedBleBoxDevice,
)
from app.clients.pstryk import (
    PstrykAPIError,
    PstrykAuthError,
    PstrykClient,
    PstrykRateLimitError,
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


POLL_HISTORY_DAYS = 7


async def pstryk_poll_job() -> None:
    """Pull the last 7 days + next 24 h of Pstryk metrics, upsert.

    The window is intentionally wider than just "the last hour" — Pstryk
    sometimes reports meter values for an hour with a delay of a day or
    two (smart-meter sync lag). A 7-day rolling window means every
    hourly tick refreshes the past week's worth of data, so any late
    fills land in the local DB without needing a manual refresh.
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
        window_start = now - timedelta(days=POLL_HISTORY_DAYS)
        window_end = now + timedelta(hours=24)

        async with PstrykClient(api_key=api_key) as client:
            payload = await client.fetch_unified_metrics(window_start, window_end)
        prices = parse_hourly_prices(payload)
        if not prices:
            logger.warning("Pstryk poll returned no parseable prices")
            state.set_pstryk_error("Pstryk returned no price data for the polling window.")
            return

        with Session(engine) as session:
            written = upsert_pstryk_prices(session, prices)
        logger.info("Pstryk poll: refreshed %d hourly rows over the last 7 d + 24 h", written)
        state.set_pstryk_error(None)
    except PstrykAuthError as exc:
        logger.warning("Pstryk auth failed: %s", exc)
        state.set_pstryk_error("Invalid Pstryk API key (HTTP 401/403). Update it in /settings.")
    except PstrykRateLimitError as exc:
        logger.warning("Pstryk rate limit: %s", exc)
        state.set_pstryk_error(
            "Pstryk rate limit hit (3 req/h per endpoint). Lower the poll cadence in /settings."
        )
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        logger.warning("Pstryk HTTP %s: %s", code, exc)
        if code == 404:
            state.set_pstryk_error(
                "Pstryk meter / contract not found (HTTP 404). Check the API key's account."
            )
        else:
            state.set_pstryk_error(f"Pstryk request failed: HTTP {code}.")
    except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as exc:
        logger.warning("Pstryk network error: %s", exc)
        state.set_pstryk_error(f"Cannot reach api.pstryk.pl: {type(exc).__name__}.")
    except PstrykAPIError as exc:
        logger.warning("Pstryk poll API error: %s", exc)
        state.set_pstryk_error(f"Pstryk API error: {exc}")
    except Exception as exc:
        logger.exception("Pstryk poll job crashed")
        state.set_pstryk_error(f"Unexpected error: {type(exc).__name__}.")


BACKFILL_MAX_YEARS = 5
BACKFILL_CHUNK_DAYS = 7
BACKFILL_STOP_AFTER_EMPTY_CHUNKS = 2
BACKFILL_INTER_CHUNK_DELAY_S = 3
# Each chunk window covers BACKFILL_CHUNK_DAYS × 24 hours; require the
# DB to hold at least this fraction of those hours before considering
# the chunk hydrated. Catches misaligned-window leftovers.
BACKFILL_HYDRATED_THRESHOLD = 0.95


async def pstryk_backfill_all_job() -> None:
    """At startup: ensure all available history of Pstryk metrics
    (price + kwh + cost) is stored locally, plus the next 24 h of
    forecast.

    The job walks 7-day chunks backwards from "now" up to a 5-year
    cap. It stops early after `BACKFILL_STOP_AFTER_EMPTY_CHUNKS`
    consecutive chunks return zero rows (typically meaning Pstryk has
    no contract / no meter data that far back). The skip-logic keeps
    the job idempotent across restarts — chunks that already carry
    both kwh_import and cost_pln are not re-fetched.
    """
    try:
        with Session(engine) as session:
            view = svc.get_view(session)
            if not view.is_configured():
                return
            api_key = svc.get_plaintext(session, svc.PSTRYK_API_KEY)
        if not api_key:
            return

        state.backfill_start()
        now = datetime.now(UTC)
        # Day-align the chunk anchor to UTC midnight so every backfill
        # run walks the same windows; without this, a noon-run vs an
        # evening-run produce shifted windows that overlap each other,
        # leaving the DB with sparse coverage that the skip-logic
        # mistakes for "fully hydrated".
        end_anchor = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        async with PstrykClient(api_key=api_key) as client:
            # Forecast window — always fetched to refresh forward prices.
            try:
                payload = await client.fetch_unified_metrics(now, now + timedelta(days=1))
                prices = parse_hourly_prices(payload)
                with Session(engine) as session:
                    upsert_pstryk_prices(session, prices)
                state.backfill_progress(
                    f"Downloading forecast ({len(prices)} hours)...", len(prices)
                )
                logger.info("Backfill forecast chunk: %d rows", len(prices))
            except PstrykAPIError as exc:
                logger.warning("Backfill forecast chunk failed: %s", exc)

            # History walk back from the day-aligned anchor. Stop after
            # a couple of consecutive empty chunks (meter has no data
            # that far back).
            consecutive_empty = 0
            chunks_fetched = 0
            max_chunks = (BACKFILL_MAX_YEARS * 366) // BACKFILL_CHUNK_DAYS + 1
            for i in range(max_chunks):
                window_end = end_anchor - timedelta(days=i * BACKFILL_CHUNK_DAYS)
                window_start = window_end - timedelta(days=BACKFILL_CHUNK_DAYS)
                if _chunk_has_full_kwh(window_start, window_end):
                    logger.info(
                        "Backfill %s..%s already hydrated, skipping",
                        window_start.date(),
                        window_end.date(),
                    )
                    state.backfill_progress(
                        f"Checking {window_start.date()} (already downloaded)...", 0
                    )
                    consecutive_empty = 0
                    continue
                # Defensive sleep between successful fetches to stay
                # under whatever Pstryk's actual rate limit is.
                if chunks_fetched > 0:
                    await asyncio.sleep(BACKFILL_INTER_CHUNK_DELAY_S)
                try:
                    payload = await client.fetch_unified_metrics(window_start, window_end)
                except PstrykAPIError as exc:
                    logger.warning(
                        "Backfill chunk %s..%s failed: %s",
                        window_start.date(),
                        window_end.date(),
                        exc,
                    )
                    continue
                chunks_fetched += 1
                prices = parse_hourly_prices(payload)
                rows_with_kwh = sum(1 for p in prices if p.kwh_import is not None)
                with Session(engine) as session:
                    upsert_pstryk_prices(session, prices)
                state.backfill_progress(
                    f"Downloading {window_start.date()} → {window_end.date()} ({len(prices)} hours)...",
                    len(prices),
                )
                logger.info(
                    "Backfill %s..%s: %d rows (%d with kwh)",
                    window_start.date(),
                    window_end.date(),
                    len(prices),
                    rows_with_kwh,
                )
                if rows_with_kwh == 0:
                    consecutive_empty += 1
                    if consecutive_empty >= BACKFILL_STOP_AFTER_EMPTY_CHUNKS:
                        logger.info(
                            "Backfill stopped after %d consecutive empty chunks (history exhausted)",
                            consecutive_empty,
                        )
                        break
                else:
                    consecutive_empty = 0

        state.backfill_done(
            f"Done. Downloaded {state.backfill_rows_loaded} hours "
            f"in {state.backfill_chunks_done} chunks."
        )
    except Exception as exc:
        logger.exception("Pstryk backfill crashed")
        state.backfill_failed(f"Download error: {exc}")


def _chunk_has_full_kwh(window_start: datetime, window_end: datetime) -> bool:
    """True if the window appears fully hydrated.

    Three conditions must all hold:
    1. The number of rows in the window is at least
       BACKFILL_HYDRATED_THRESHOLD (95 %) of the expected hour count.
    2. No row in the window has NULL kwh_import or cost_pln.
    3. The chunk has at least one row (i.e. it isn't completely empty,
       which would mean we never fetched it).

    The row-count check guards against an earlier non-day-aligned
    backfill leaving sparse coverage that the second condition alone
    would mistake for "hydrated".
    """
    from sqlalchemy import func

    start = window_start.replace(tzinfo=None) if window_start.tzinfo else window_start
    end = window_end.replace(tzinfo=None) if window_end.tzinfo else window_end
    expected_hours = max(1, int((end - start).total_seconds() / 3600))
    with Session(engine) as session:
        row_count = session.exec(
            select(func.count())  # type: ignore[arg-type]
            .select_from(PstrykPrice)
            .where(PstrykPrice.ts_utc >= start)
            .where(PstrykPrice.ts_utc < end)
        ).one()
        if isinstance(row_count, tuple):
            row_count = row_count[0]
        if row_count == 0:
            return False
        if row_count < expected_hours * BACKFILL_HYDRATED_THRESHOLD:
            return False
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
        return missing is None


async def blebox_live_job() -> None:
    """Poll BleBox and refresh the in-memory live cache.

    Runs every `blebox_live_seconds` (default 5 s). The 60 s persist
    job reads the cached reading rather than re-hitting the device.
    """
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
        state.set_blebox_error(None)
    except UnsupportedBleBoxDevice as exc:
        logger.warning("BleBox unsupported device: %s", exc)
        state.set_blebox_error(f"BleBox device not supported: {exc}")
    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        logger.warning("BleBox connect failure: %s", exc)
        state.set_blebox_error(f"Cannot reach BleBox at {host}:{port}. Check the IP in /settings.")
    except httpx.ReadTimeout:
        logger.warning("BleBox read timeout")
        state.set_blebox_error(f"BleBox at {host}:{port} did not respond in time.")
    except httpx.HTTPStatusError as exc:
        logger.warning("BleBox HTTP %s: %s", exc.response.status_code, exc)
        state.set_blebox_error(
            f"BleBox returned HTTP {exc.response.status_code}. Firmware/path mismatch?"
        )
    except BleBoxError as exc:
        logger.warning("BleBox live job error: %s", exc)
        state.set_blebox_error(f"BleBox error: {exc}")
    except Exception as exc:
        logger.exception("BleBox live job crashed")
        state.set_blebox_error(f"Unexpected error: {type(exc).__name__}.")


def blebox_persist_job() -> None:
    """Persist the most recent cached BleBox reading to MeterReading."""
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
        pstryk_backfill_all_job,
        next_run_time=datetime.now(UTC) + timedelta(seconds=10),
        id="pstryk_backfill",
        max_instances=1,
    )
    # Daily idempotent re-run: fills gaps left by transient failures
    # (429, network blips) on the original startup walk. Skip-logic
    # makes already-hydrated chunks free; only "missing kwh" chunks
    # get refetched, so this costs at most a handful of API calls.
    sched.add_job(
        pstryk_backfill_all_job,
        CronTrigger(hour=3, minute=0, timezone=view.tz),
        id="pstryk_backfill_daily",
        max_instances=1,
        coalesce=True,
    )
    return sched
