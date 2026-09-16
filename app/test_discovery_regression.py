"""Regression test: post-migration HA Discovery payloads byte-for-byte equal
the pre-migration output.

The expected payloads below were captured from the pre-migration
``main.py`` (commit before this PR) by running ``discovery_payloads()``
against a fixed sensor + gateway pair and JSON-serializing with
``json.dumps(payload, sort_keys=True)``.

Pin failure here means the migration accidentally changed something
HA-visible — fix the toolkit or the call site rather than this fixture.

This file lives in-tree because there is no other test framework wired
up in the Govee project, and the regression must run wherever this main
module is importable. Run with::

    cd app
    GOVEE_EMAIL=x GOVEE_PASSWORD=x GOVEE_CLIENT_ID=x MQTT_PASSWORD=x \
        python -m pytest test_discovery_regression.py -q
"""

from __future__ import annotations

import json
import os
import sys
import types
import unittest


# The module reads required env vars at import time. Stub them out before
# importing main so the test doesn't require real credentials.
_REQUIRED = {
    "GOVEE_EMAIL": "x@example.com",
    "GOVEE_PASSWORD": "test",
    "GOVEE_CLIENT_ID": "test-client",
    "MQTT_PASSWORD": "test",
}
for k, v in _REQUIRED.items():
    os.environ.setdefault(k, v)


# Stub `paho` and `requests` so import-time doesn't require them being
# installed when running the test outside the Docker image.
def _stub_module(name: str, **attrs: object) -> None:
    if name in sys.modules:
        return
    mod = types.ModuleType(name)
    for attr_name, attr_val in attrs.items():
        setattr(mod, attr_name, attr_val)
    sys.modules[name] = mod


_stub_module("requests")
sys.modules["requests"].RequestException = Exception  # type: ignore[attr-defined]
_stub_module("paho")
_stub_module("paho.mqtt")
mqtt_stub = types.ModuleType("paho.mqtt.client")


class _FakeCallbackAPIVersion:
    VERSION2 = 2


class _FakeMqttClient:
    def __init__(self, *_, **__):
        pass


mqtt_stub.CallbackAPIVersion = _FakeCallbackAPIVersion
mqtt_stub.Client = _FakeMqttClient
sys.modules["paho.mqtt.client"] = mqtt_stub

# Stub ha_mqtt_bridge if the toolkit isn't installed (CI / local dev
# outside the Docker image) — point at the in-repo source.
if "ha_mqtt_bridge" not in sys.modules:
    here = os.path.dirname(os.path.abspath(__file__))
    toolkit = os.path.normpath(
        os.path.join(here, "..", "..", "..", "_shared", "ha-mqtt-bridge-toolkit")
    )
    sys.path.insert(0, toolkit)

import main  # noqa: E402


SENSOR = main.Sensor(
    device="00:00:5e:00:53:01",
    name="Kitchen Leak",
    battery=87,
    online=True,
    gwonline=True,
    last_time=1704067200000,
    firmware="1.0.7",
    slot=3,
)

GATEWAY = main.Gateway(
    device="00:00:5e:00:53:02",
    name="Govee Water Sensor Gateway",
    online=True,
    last_heartbeat=1704067200000,
    firmware="2.0.1",
    mac="00:00:5E:00:53:03",
)


# Captured pre-migration output. Order matters because publish ordering
# is part of the contract — discovery_published[i].topic was previously
# emitted in this exact sequence.
EXPECTED: list[tuple[str, dict]] = [
    # ---- sensor entities ----
    (
        "homeassistant/binary_sensor/govee_h5054_00:00:5e:00:53:01/leak/config",
        {
            "name": "Leak",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_leak",
            "object_id": "govee_h5054_00:00:5e:00:53:01_leak",
            "state_topic": "govee/leak/00:00:5e:00:53:01/leak",
            "json_attributes_topic": "govee/leak/00:00:5e:00:53:01/leak/attrs",
            "device_class": "moisture",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5054_00:00:5e:00:53:01/battery/config",
        {
            "name": "Battery",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_battery",
            "object_id": "govee_h5054_00:00:5e:00:53:01_battery",
            "state_topic": "govee/leak/00:00:5e:00:53:01/battery",
            "device_class": "battery",
            "unit_of_measurement": "%",
            "state_class": "measurement",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/binary_sensor/govee_h5054_00:00:5e:00:53:01/online/config",
        {
            "name": "Online",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_online",
            "object_id": "govee_h5054_00:00:5e:00:53:01_online",
            "state_topic": "govee/leak/00:00:5e:00:53:01/online",
            "device_class": "connectivity",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5054_00:00:5e:00:53:01/last_leak/config",
        {
            "name": "Last leak",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_last_leak",
            "object_id": "govee_h5054_00:00:5e:00:53:01_last_leak",
            "state_topic": "govee/leak/00:00:5e:00:53:01/last_leak",
            "device_class": "timestamp",
            "icon": "mdi:water-alert",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5054_00:00:5e:00:53:01/firmware/config",
        {
            "name": "Firmware",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_firmware",
            "object_id": "govee_h5054_00:00:5e:00:53:01_firmware",
            "state_topic": "govee/leak/00:00:5e:00:53:01/firmware",
            "entity_category": "diagnostic",
            "icon": "mdi:chip",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5054_00:00:5e:00:53:01/slot/config",
        {
            "name": "Gateway slot",
            "unique_id": "govee_h5054_00:00:5e:00:53:01_slot",
            "object_id": "govee_h5054_00:00:5e:00:53:01_slot",
            "state_topic": "govee/leak/00:00:5e:00:53:01/slot",
            "entity_category": "diagnostic",
            "icon": "mdi:numeric",
            "device": {
                "identifiers": ["govee_h5054_00:00:5e:00:53:01"],
                "name": "Kitchen Leak",
                "manufacturer": "Govee",
                "model": "H5054",
                "sw_version": "1.0.7",
                "via_device": "govee_h5040_00:00:5e:00:53:02",
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    # ---- gateway entities ----
    (
        "homeassistant/binary_sensor/govee_h5040_00:00:5e:00:53:02/online/config",
        {
            "name": "Online",
            "unique_id": "govee_h5040_00:00:5e:00:53:02_online",
            "object_id": "govee_h5040_00:00:5e:00:53:02_online",
            "state_topic": "govee/leak/00:00:5e:00:53:02/online",
            "device_class": "connectivity",
            "payload_on": "ON",
            "payload_off": "OFF",
            "device": {
                "identifiers": ["govee_h5040_00:00:5e:00:53:02"],
                "name": "Govee Water Sensor Gateway",
                "manufacturer": "Govee",
                "model": "H5040",
                "sw_version": "2.0.1",
                "connections": [["mac", "00:00:5e:00:53:03"]],
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5040_00:00:5e:00:53:02/last_heartbeat/config",
        {
            "name": "Last heartbeat",
            "unique_id": "govee_h5040_00:00:5e:00:53:02_last_heartbeat",
            "object_id": "govee_h5040_00:00:5e:00:53:02_last_heartbeat",
            "state_topic": "govee/leak/00:00:5e:00:53:02/last_heartbeat",
            "device_class": "timestamp",
            "icon": "mdi:heart-pulse",
            "device": {
                "identifiers": ["govee_h5040_00:00:5e:00:53:02"],
                "name": "Govee Water Sensor Gateway",
                "manufacturer": "Govee",
                "model": "H5040",
                "sw_version": "2.0.1",
                "connections": [["mac", "00:00:5e:00:53:03"]],
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5040_00:00:5e:00:53:02/firmware/config",
        {
            "name": "Firmware",
            "unique_id": "govee_h5040_00:00:5e:00:53:02_firmware",
            "object_id": "govee_h5040_00:00:5e:00:53:02_firmware",
            "state_topic": "govee/leak/00:00:5e:00:53:02/firmware",
            "entity_category": "diagnostic",
            "icon": "mdi:chip",
            "device": {
                "identifiers": ["govee_h5040_00:00:5e:00:53:02"],
                "name": "Govee Water Sensor Gateway",
                "manufacturer": "Govee",
                "model": "H5040",
                "sw_version": "2.0.1",
                "connections": [["mac", "00:00:5e:00:53:03"]],
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
    (
        "homeassistant/sensor/govee_h5040_00:00:5e:00:53:02/mac/config",
        {
            "name": "MAC",
            "unique_id": "govee_h5040_00:00:5e:00:53:02_mac",
            "object_id": "govee_h5040_00:00:5e:00:53:02_mac",
            "state_topic": "govee/leak/00:00:5e:00:53:02/mac",
            "entity_category": "diagnostic",
            "icon": "mdi:network",
            "device": {
                "identifiers": ["govee_h5040_00:00:5e:00:53:02"],
                "name": "Govee Water Sensor Gateway",
                "manufacturer": "Govee",
                "model": "H5040",
                "sw_version": "2.0.1",
                "connections": [["mac", "00:00:5e:00:53:03"]],
            },
            "availability_topic": "govee/leak/bridge/online",
            "payload_available": "online",
            "payload_not_available": "offline",
        },
    ),
]


def _topic_for(component: str, unique_id: str) -> str:
    """Mirror ``ThreadedPublisher.publish_discovery``'s topic construction
    so this test pins exactly what the publisher will emit."""
    return f"homeassistant/{component}/{unique_id}/config"


class TestDiscoveryRegression(unittest.TestCase):
    """Verify ``discovery_specs()`` (Phase 3 — returns (component,
    unique_id, payload) triples for ThreadedPublisher.publish_discovery)
    produces the same on-the-wire discovery topics and JSON payloads
    that the pre-Phase-1 ``discovery_payloads()`` (which returned
    (topic, payload) tuples) emitted."""

    def test_spec_count(self) -> None:
        out = main.discovery_specs([SENSOR], GATEWAY)
        self.assertEqual(
            len(out),
            len(EXPECTED),
            msg=f"got {len(out)} specs, expected {len(EXPECTED)}",
        )

    def test_topics_match(self) -> None:
        out = main.discovery_specs([SENSOR], GATEWAY)
        for i, ((component, unique_id, _), (etopic, _)) in enumerate(zip(out, EXPECTED)):
            got = _topic_for(component, unique_id)
            self.assertEqual(got, etopic, msg=f"topic mismatch at index {i}")

    def test_payloads_byte_for_byte(self) -> None:
        out = main.discovery_specs([SENSOR], GATEWAY)
        for i, ((_, _, payload), (_, expected)) in enumerate(zip(out, EXPECTED)):
            got = json.dumps(payload, sort_keys=True)
            want = json.dumps(expected, sort_keys=True)
            self.assertEqual(got, want, msg=f"payload mismatch at index {i}")

    def test_no_gateway(self) -> None:
        # When no gateway present, sensor `via_device` must be omitted
        # (not None) — the device_block builder drops None fields.
        out = main.discovery_specs([SENSOR], None)
        for _, _, payload in out:
            self.assertNotIn("via_device", payload["device"])


if __name__ == "__main__":
    unittest.main()
