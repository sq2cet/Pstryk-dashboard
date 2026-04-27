"""Async client for BleBox energy-metering devices on the LAN.

The exact device model is unknown until the user provides the IP.
The flow is:

1. Probe `/api/device/state` to learn the device `type`.
2. Dispatch to the appropriate state endpoint:
   - `switchBox`, `switchBoxD` → `/api/relay/extended/state`
   - `multiSensor` → `/state`
3. Parse active power (W) and cumulative energy (kWh) from the response.

The BleBox MultiSensor (e.g. the Pstryk-branded `PstrykEnergyMeter`)
reports a 3-phase reading as four sensor groups:
  `id=0` = household totals, `id=1/2/3` = phases L1/L2/L3.
The dashboard cares about the totals only, so the parser keeps `id=0`
and ignores per-phase rows. `forwardActiveEnergy` is reported in Wh and
divided by 1000 to convert to the kWh the rest of the app uses.

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
class PhaseReading:
    active_power_w: float | None = None
    voltage_v: float | None = None
    current_a: float | None = None


@dataclass(frozen=True)
class BleBoxReading:
    ts_utc: datetime
    active_power_w: float | None
    energy_kwh_total: float | None
    raw: dict
    reverse_energy_kwh_total: float | None = None
    forward_reactive_energy_varh: float | None = None
    reverse_reactive_energy_varh: float | None = None
    apparent_energy_vah: float | None = None
    apparent_power_va: float | None = None
    reactive_power_var: float | None = None
    voltage_v: float | None = None
    current_a: float | None = None
    frequency_hz: float | None = None
    phase_l1: PhaseReading | None = None
    phase_l2: PhaseReading | None = None
    phase_l3: PhaseReading | None = None

    @property
    def power_factor(self) -> float | None:
        """|active_power| / apparent_power, in [0, 1]. None if either missing."""
        if (
            self.active_power_w is None
            or self.apparent_power_va is None
            or self.apparent_power_va == 0
        ):
            return None
        return min(abs(self.active_power_w) / self.apparent_power_va, 1.0)


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
            payload = await self._get("/state")
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
    """MultiSensor parser for the Pstryk-branded `PstrykEnergyMeter`.

    Sensors come in groups identified by `id`: `id=0` is the household
    aggregate, `id=1/2/3` are per-phase L1/L2/L3.

    Unit conventions (verified against a real device, apiLevel 20241124):
    - `activePower`, `apparentPower`, `reactivePower` — whole W / VA / var
    - `forwardActiveEnergy`, `reverseActiveEnergy` — Wh (divided to kWh)
    - `voltage` — decivolts (divided by 10)
    - `current` — milliamps (divided by 1000)
    - `frequency` — millihertz (divided by 1000)
    """
    all_sensors = (payload.get("multiSensor") or {}).get("sensors") or payload.get("sensors") or []
    by_id: dict[int, list[dict]] = {}
    for s in all_sensors:
        if isinstance(s, dict) and isinstance(s.get("id"), int):
            by_id.setdefault(s["id"], []).append(s)

    aggregate = by_id.get(0, [])
    forward_wh = _find_sensor(aggregate, "forwardActiveEnergy")
    reverse_wh = _find_sensor(aggregate, "reverseActiveEnergy")
    forward_react_wh = _find_sensor(aggregate, "forwardReactiveEnergy")
    reverse_react_wh = _find_sensor(aggregate, "reverseReactiveEnergy")
    apparent_wh = _find_sensor(aggregate, "apparentEnergy")
    voltage_dv = _find_sensor(aggregate, "voltage")
    current_ma = _find_sensor(aggregate, "current")
    freq_mhz = _find_sensor(aggregate, "frequency")

    def _phase(sid: int) -> PhaseReading | None:
        sensors = by_id.get(sid)
        if not sensors:
            return None
        v_dv = _find_sensor(sensors, "voltage")
        c_ma = _find_sensor(sensors, "current")
        return PhaseReading(
            active_power_w=_find_sensor(sensors, "activePower"),
            voltage_v=(v_dv / 10.0) if v_dv is not None else None,
            current_a=(c_ma / 1000.0) if c_ma is not None else None,
        )

    return BleBoxReading(
        ts_utc=_now_utc(),
        active_power_w=_find_sensor(aggregate, "activePower"),
        energy_kwh_total=(forward_wh / 1000.0) if forward_wh is not None else None,
        reverse_energy_kwh_total=(reverse_wh / 1000.0) if reverse_wh is not None else None,
        forward_reactive_energy_varh=forward_react_wh,
        reverse_reactive_energy_varh=reverse_react_wh,
        apparent_energy_vah=apparent_wh,
        apparent_power_va=_find_sensor(aggregate, "apparentPower"),
        reactive_power_var=_find_sensor(aggregate, "reactivePower"),
        voltage_v=(voltage_dv / 10.0) if voltage_dv is not None else None,
        current_a=(current_ma / 1000.0) if current_ma is not None else None,
        frequency_hz=(freq_mhz / 1000.0) if freq_mhz is not None else None,
        phase_l1=_phase(1),
        phase_l2=_phase(2),
        phase_l3=_phase(3),
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
