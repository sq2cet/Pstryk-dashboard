"""Async client for BleBox energy-metering devices on the LAN.

The exact device model is unknown until the user provides the IP.
The flow is:

1. Probe `/api/device/state` to learn the device `type`.
2. Dispatch to the appropriate state endpoint:
   - `switchBox`, `switchBoxD` → `/api/relay/extended/state`
   - `multiSensor` → `/api/multiSensor/state`
3. Parse active power (W) and cumulative energy (kWh) from the response.

Devices on the LAN do not require authentication.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 5.0


class BleBoxError(Exception):
    pass


class UnsupportedBleBoxDevice(BleBoxError):
    pass


@dataclass(frozen=True)
class BleBoxDevice:
    type: str
    name: str
    api_level: str | None
    raw: dict


@dataclass(frozen=True)
class BleBoxReading:
    ts_utc: datetime
    active_power_w: float | None
    energy_kwh_total: float | None
    raw: dict


_RELAY_TYPES = {"switchBox", "switchBoxD"}
_MULTISENSOR_TYPES = {"multiSensor"}


class BleBoxClient:
    def __init__(
        self,
        host: str,
        port: int = 80,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        if not host:
            raise ValueError("host is required")
        scheme = "http"
        self._client = httpx.AsyncClient(
            base_url=f"{scheme}://{host}:{port}",
            timeout=timeout,
        )
        self._device: BleBoxDevice | None = None

    async def __aenter__(self) -> BleBoxClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def probe(self, force: bool = False) -> BleBoxDevice:
        if self._device is not None and not force:
            return self._device
        response = await self._client.get("/api/device/state")
        response.raise_for_status()
        payload = response.json()
        device = payload.get("device") or payload
        type_ = device.get("type") or device.get("product")
        if not isinstance(type_, str):
            raise UnsupportedBleBoxDevice(f"could not identify device type from {payload!r}")
        self._device = BleBoxDevice(
            type=type_,
            name=str(device.get("deviceName") or device.get("name") or type_),
            api_level=str(device["apiLevel"]) if "apiLevel" in device else None,
            raw=payload,
        )
        return self._device

    async def read_state(self) -> BleBoxReading:
        device = await self.probe()
        if device.type in _RELAY_TYPES:
            payload = await self._get("/api/relay/extended/state")
            return parse_relay_state(payload)
        if device.type in _MULTISENSOR_TYPES:
            payload = await self._get("/api/multiSensor/state")
            return parse_multisensor_state(payload)
        raise UnsupportedBleBoxDevice(
            f"BleBox device type {device.type!r} is not supported by the energy parser"
        )

    async def _get(self, path: str) -> dict:
        response = await self._client.get(path)
        response.raise_for_status()
        return response.json()


def parse_relay_state(payload: dict) -> BleBoxReading:
    """SwitchBox / SwitchBoxD parser.

    `sensors[type=activePower].value` is in Watts.
    `powerMeasuring.powerConsumption[].value` is kWh accumulated over `periodS`;
    when multiple buckets are reported we take the longest period as the
    "current cumulative" approximation.
    """
    active_power = _find_sensor(payload.get("sensors") or [], "activePower")

    consumption = (payload.get("powerMeasuring") or {}).get("powerConsumption") or []
    energy_total: float | None = None
    if consumption:
        longest = max(consumption, key=lambda b: int(b.get("periodS") or 0))
        if isinstance(longest.get("value"), (int, float)):
            energy_total = float(longest["value"])

    return BleBoxReading(
        ts_utc=_now_utc(),
        active_power_w=active_power,
        energy_kwh_total=energy_total,
        raw=payload,
    )


def parse_multisensor_state(payload: dict) -> BleBoxReading:
    """MultiSensor parser.

    The MultiSensor reports a heterogeneous list under `multiSensor.sensors`;
    each entry has a `type` (`activePower`, `energy`, `voltage`, ...) and a
    `value`. Schema details vary by `apiLevel`; this parser is intentionally
    forgiving.
    """
    sensors = (payload.get("multiSensor") or {}).get("sensors") or payload.get("sensors") or []
    return BleBoxReading(
        ts_utc=_now_utc(),
        active_power_w=_find_sensor(sensors, "activePower"),
        energy_kwh_total=_find_sensor(sensors, "energy", "energyTotal", "totalEnergy"),
        raw=payload,
    )


def _find_sensor(sensors: list[dict], *types: str) -> float | None:
    wanted = {t.lower() for t in types}
    for sensor in sensors:
        if not isinstance(sensor, dict):
            continue
        kind = str(sensor.get("type") or "").lower()
        if kind in wanted and isinstance(sensor.get("value"), (int, float)):
            return float(sensor["value"])
    return None


def _now_utc() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def coerce_payload(raw: Any) -> dict:
    """Defensive helper for callers that may receive non-dict bodies."""
    if isinstance(raw, dict):
        return raw
    raise BleBoxError(f"unexpected payload type {type(raw).__name__}")
