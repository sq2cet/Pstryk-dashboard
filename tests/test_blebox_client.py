import json
from pathlib import Path

import httpx
import pytest
import respx

from app.clients.blebox import (
    BleBoxClient,
    UnsupportedBleBoxDevice,
    parse_multisensor_state,
    parse_relay_state,
)

FIXTURE = json.loads((Path(__file__).parent / "fixtures" / "blebox_state.json").read_text())


@respx.mock
async def test_probe_identifies_switchboxd() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["device_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        device = await client.probe()
    assert device.type == "switchBoxD"
    assert device.name == "kitchen-meter"
    assert device.api_level == "20200831"


@respx.mock
async def test_probe_caches_result() -> None:
    route = respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["device_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        await client.probe()
        await client.probe()
    assert route.call_count == 1


@respx.mock
async def test_read_state_switchboxd_dispatches_and_parses() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["device_state"]),
    )
    relay_route = respx.get("http://192.168.1.50:80/api/relay/extended/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["relay_extended_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        reading = await client.read_state()
    assert relay_route.called
    assert reading.active_power_w == 520.0
    assert reading.energy_kwh_total == 0.521  # longest periodS bucket


@respx.mock
async def test_read_state_unsupported_device() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(
            200,
            json={"device": {"type": "shutterBox", "product": "shutterBox"}},
        ),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        with pytest.raises(UnsupportedBleBoxDevice):
            await client.read_state()


def test_parse_relay_state_handles_missing_power_section() -> None:
    payload = {"relays": [{"relay": 0, "state": 0}], "sensors": []}
    reading = parse_relay_state(payload)
    assert reading.active_power_w is None
    assert reading.energy_kwh_total is None


def test_parse_multisensor_state_extracts_power_and_energy() -> None:
    reading = parse_multisensor_state(FIXTURE["multisensor_state"])
    assert reading.active_power_w == 1234.5
    assert reading.energy_kwh_total == 87.65


def test_parse_multisensor_state_handles_alternate_energy_key() -> None:
    payload = {
        "multiSensor": {
            "sensors": [
                {"type": "activePower", "value": 100},
                {"type": "energyTotal", "value": 42.0},
            ]
        }
    }
    reading = parse_multisensor_state(payload)
    assert reading.energy_kwh_total == 42.0


def test_blebox_client_rejects_empty_host() -> None:
    with pytest.raises(ValueError):
        BleBoxClient(host="")
