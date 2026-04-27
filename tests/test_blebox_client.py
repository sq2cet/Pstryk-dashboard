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
async def test_probe_identifies_multisensor_pstryk_meter() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["device_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        device = await client.probe()
    assert device.type == "multiSensor"
    assert device.name == "Pstryk"
    assert device.api_level == "20241124"


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
async def test_read_state_multisensor_uses_state_endpoint() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["device_state"]),
    )
    state_route = respx.get("http://192.168.1.50:80/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["multisensor_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        reading = await client.read_state()
    assert state_route.called
    # id=0 totals: 520 W, 109443 Wh = 109.443 kWh.
    assert reading.active_power_w == 520.0
    assert reading.energy_kwh_total == pytest.approx(109.443)


@respx.mock
async def test_read_state_switchboxd_relay_path_unchanged() -> None:
    respx.get("http://192.168.1.50:80/api/device/state").mock(
        return_value=httpx.Response(
            200, json={"device": {"type": "switchBoxD", "product": "switchBoxD"}}
        ),
    )
    relay_route = respx.get("http://192.168.1.50:80/api/relay/extended/state").mock(
        return_value=httpx.Response(200, json=FIXTURE["relay_extended_state"]),
    )
    async with BleBoxClient(host="192.168.1.50") as client:
        reading = await client.read_state()
    assert relay_route.called
    assert reading.active_power_w == 520.0
    assert reading.energy_kwh_total == 0.521


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


def test_parse_multisensor_filters_to_id0_and_converts_units() -> None:
    reading = parse_multisensor_state(FIXTURE["multisensor_state"])
    # Only id=0 sensors should drive the result, regardless of how many
    # per-phase entries exist.
    assert reading.active_power_w == 520.0
    assert reading.energy_kwh_total == pytest.approx(109.443)


def test_parse_multisensor_with_no_id0_yields_none() -> None:
    payload = {
        "multiSensor": {
            "sensors": [
                {"id": 1, "type": "activePower", "value": 100},
                {"id": 1, "type": "forwardActiveEnergy", "value": 1000},
            ]
        }
    }
    reading = parse_multisensor_state(payload)
    assert reading.active_power_w is None
    assert reading.energy_kwh_total is None


def test_parse_relay_state_handles_missing_power_section() -> None:
    payload = {"relays": [{"relay": 0, "state": 0}], "sensors": []}
    reading = parse_relay_state(payload)
    assert reading.active_power_w is None
    assert reading.energy_kwh_total is None


def test_blebox_client_rejects_empty_host() -> None:
    with pytest.raises(ValueError):
        BleBoxClient(host="")
