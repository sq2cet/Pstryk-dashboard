"""Process-local cache for the live tile and the live-power chart.

The 5-second BleBox poll stores the most recent reading here in memory;
the HTML fragment for the live tile reads from this module so the
browser's 5 s tick does not have to talk to the LAN device. The 60 s
persist job consumes the same cached reading and writes a row to
`MeterReading`, so a fresh reading is fetched at most once per 5 s.

`recent_readings` is a bounded deque of the last ~hour of 5 s-cadence
readings (default poll = 5 s, 720 entries ≈ 60 minutes). The
live-power chart is drawn from this buffer — fine-grained per-phase
data for the most recent window with no DB write cost.

Intentionally not persisted — the dashboard recovers on the next
scheduler fire after a restart.
"""

from __future__ import annotations

from collections import deque

from app.clients.blebox import BleBoxReading

LIVE_BUFFER_MAX = 720

last_reading: BleBoxReading | None = None
recent_readings: deque[BleBoxReading] = deque(maxlen=LIVE_BUFFER_MAX)


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
