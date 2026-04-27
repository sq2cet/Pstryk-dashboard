"""Process-local cache for the live tile.

The 5-second BleBox poll stores the most recent reading here in memory;
the HTML fragment for the live tile reads from this module so the
browser's 5 s tick does not have to talk to the LAN device. The 60 s
persist job consumes the same cached reading and writes a row to
`MeterReading`, so a fresh reading is fetched at most once per 5 s.

Intentionally not persisted — the dashboard recovers on the next
scheduler fire after a restart.
"""

from __future__ import annotations

from app.clients.blebox import BleBoxReading

last_reading: BleBoxReading | None = None


def set_last_reading(reading: BleBoxReading | None) -> None:
    global last_reading
    last_reading = reading
