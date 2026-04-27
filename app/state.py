"""Process-local cache for the live tile, live-power chart, and the
backfill status banner.

The 5-second BleBox poll stores the most recent reading here in memory;
the HTML fragment for the live tile reads from this module so the
browser's 5 s tick does not have to talk to the LAN device. The 60 s
persist job consumes the same cached reading and writes a row to
`MeterReading`, so a fresh reading is fetched at most once per 5 s.

`recent_readings` is a bounded deque of the last ~hour of 5 s-cadence
readings. `backfill_*` exposes the state of the startup backfill job
so the dashboard can show a "downloading historical data" banner.

Intentionally not persisted — recovers on the next scheduler fire
after a restart.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime

from app.clients.blebox import BleBoxReading

LIVE_BUFFER_MAX = 720

last_reading: BleBoxReading | None = None
recent_readings: deque[BleBoxReading] = deque(maxlen=LIVE_BUFFER_MAX)

# Backfill job state. The /partials/backfill-status endpoint reads
# these and renders a banner; on "complete" the endpoint returns an
# empty body so the HTMX target collapses to nothing.
backfill_status: str = "idle"  # idle | running | complete | failed
backfill_message: str = ""
backfill_chunks_done: int = 0
backfill_rows_loaded: int = 0
backfill_started_at: datetime | None = None
backfill_finished_at: datetime | None = None


def set_last_reading(reading: BleBoxReading | None) -> None:
    global last_reading
    last_reading = reading
    if reading is not None:
        recent_readings.append(reading)


def reset_buffers() -> None:
    """Clear in-memory state. Used by tests and on settings rotation."""
    global last_reading
    last_reading = None
    recent_readings.clear()


def backfill_start() -> None:
    global backfill_status, backfill_message, backfill_chunks_done
    global backfill_rows_loaded, backfill_started_at, backfill_finished_at
    backfill_status = "running"
    backfill_message = "Pobieranie danych historycznych..."
    backfill_chunks_done = 0
    backfill_rows_loaded = 0
    backfill_started_at = datetime.utcnow()
    backfill_finished_at = None


def backfill_progress(message: str, rows_loaded_in_chunk: int) -> None:
    global backfill_message, backfill_chunks_done, backfill_rows_loaded
    backfill_chunks_done += 1
    backfill_rows_loaded += rows_loaded_in_chunk
    backfill_message = message


def backfill_done(message: str = "Dane historyczne pobrane.") -> None:
    global backfill_status, backfill_message, backfill_finished_at
    backfill_status = "complete"
    backfill_message = message
    backfill_finished_at = datetime.utcnow()


def backfill_failed(message: str) -> None:
    global backfill_status, backfill_message, backfill_finished_at
    backfill_status = "failed"
    backfill_message = message
    backfill_finished_at = datetime.utcnow()
