"""
Govee Cloud → MQTT bridge for H5040 / H5054 water leak sensors.

Polls the (undocumented) Govee account API used by the iOS app, extracts
H5054 sensor state from /bff-app/v1/device/list and H5040 gateway state from
/app/v2/gateways, and publishes Home Assistant MQTT discovery configs and
per-poll state updates.

Auth flow lifted from `wez/govee2mqtt`'s undoc_api.rs (login endpoint, header
set, request body, JWT path); device endpoints discovered by capturing the
iOS app via Proxyman. See README.md for the full story.

Leak detection: each H5054 entry carries `lastDeviceData.lastTime` (epoch ms
of the most recent leak alert; 0 if never). On startup we baseline against
the current value, then on each subsequent poll a strictly-greater value
indicates a new leak event. Leak state is held ON for LEAK_HOLD_SECONDS
after detection, then auto-cleared.

MQTT client / LWT / publish flavors, HA discovery payload assembly,
device-block construction, and ISO-8601 timestamp formatting all go
through the shared `ha_mqtt_bridge` toolkit (`ha-mqtt-bridge-toolkit`).
"""

from __future__ import annotations

import base64
import json
import os
import signal
import sys
import time
import uuid
from dataclasses import dataclass

import requests
from ha_mqtt_bridge import (
    ThreadedPublisher,
    availability_block,
    build_device_block,
    build_discovery_payload,
    configure_logging,
    epoch_ms_to_iso,
    now_ms,
    register_github_error_reporter,
    watch_ha_birth,
)


# --- production-error reporter ---------------------------------------
# Installs sys.excepthook + threading.excepthook so every uncaught
# exception flows through GitHub repository_dispatch → the Production
# Error Intake workflow → Claude auto-fix PR. Silently disabled when
# GITHUB_ERROR_TOKEN is unset (e.g. local dev).
register_github_error_reporter("govee-mqtt-bridge")
# ---------------------------------------------------------------------
GOVEE_BASE = "https://app2.govee.com"
APP_VERSION = os.environ.get("GOVEE_APP_VERSION", "7.4.21")
USER_AGENT = os.environ.get(
    "GOVEE_USER_AGENT",
    "GoveeHome/7.4.21 (com.ihoment.GoVeeSensor; build:7.4.21; iOS 26.5.0) Alamofire/4.0.0",
)

EMAIL = os.environ["GOVEE_EMAIL"]
PASSWORD = os.environ["GOVEE_PASSWORD"]
MQTT_HOST = os.environ.get("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ["MQTT_PASSWORD"]
# Off by default (current behaviour) — set MQTT_TLS=1 for a broker that
# requires TLS; MQTT_CA_FILE points at a custom CA bundle (system trust
# store is used when unset).
MQTT_TLS = os.environ.get("MQTT_TLS", "0") != "0"
MQTT_CA_FILE = os.environ.get("MQTT_CA_FILE") or None
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
LEAK_HOLD_SECONDS = int(os.environ.get("LEAK_HOLD_SECONDS", "600"))
DISCOVERY_PREFIX = os.environ.get("HA_DISCOVERY_PREFIX", "homeassistant")
TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "govee/leak")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Govee silently rejects any clientId that hasn't been registered with the
# account (returns status:454 with empty message). The iOS app does the
# registration at first install via a flow we haven't reverse-engineered, so
# the bridge has to reuse a clientId that's already known to Govee — typically
# the one from the user's iOS app, captured via Proxyman from the `clientId`
# header on any authenticated request.
CLIENT_ID = os.environ["GOVEE_CLIENT_ID"]

BRIDGE_LWT_TOPIC = f"{TOPIC_PREFIX}/bridge/online"


@dataclass
class Sensor:
    device: str
    name: str
    battery: int | None
    online: bool
    gwonline: bool
    last_time: int
    firmware: str
    slot: int | None


@dataclass
class Gateway:
    device: str
    name: str
    online: bool
    last_heartbeat: int
    firmware: str
    mac: str


def request_headers(token: str | None = None) -> dict[str, str]:
    h = {
        "appVersion": APP_VERSION,
        "clientId": CLIENT_ID,
        "clientType": "1",
        "iotVersion": "0",
        "timestamp": str(now_ms()),
        "User-Agent": USER_AGENT,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _decode_response(text: str) -> dict:
    """Govee responses are sometimes base64-wrapped JSON; handle both forms."""
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass
    decoded = base64.b64decode(text).decode("utf-8")
    return json.loads(decoded)


def login() -> tuple[str, int]:
    """Return (token, expires_at_ms). Raises on failure."""
    url = f"{GOVEE_BASE}/account/rest/account/v1/login"
    headers = {**request_headers(), "Content-Type": "application/json"}
    body = {"email": EMAIL, "password": PASSWORD, "client": CLIENT_ID}
    r = requests.post(url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    payload = _decode_response(r.text)
    if payload.get("status") != 200:
        raise RuntimeError(f"login rejected: status={payload.get('status')} message={payload.get('message')}")
    # Govee wraps the token in `client` per govee2mqtt; tolerate `data` if a
    # newer response shape ever shows up.
    block = payload.get("client") or payload.get("data") or {}
    token = block.get("token")
    if not token:
        raise RuntimeError(f"login response missing token; top-level keys={list(payload.keys())}")
    ttl_seconds = int(block.get("token_expire_cycle") or 43200)
    expires_at = now_ms() + ttl_seconds * 1000
    return token, expires_at


def fetch_devices(token: str) -> dict:
    url = f"{GOVEE_BASE}/bff-app/v1/device/list"
    r = requests.get(url, headers=request_headers(token), timeout=30)
    if r.status_code == 401:
        raise PermissionError("device list 401")
    r.raise_for_status()
    return _decode_response(r.text)


def fetch_gateways(token: str) -> dict:
    url = f"{GOVEE_BASE}/app/v2/gateways"
    r = requests.get(url, headers=request_headers(token), timeout=30)
    if r.status_code == 401:
        raise PermissionError("gateways 401")
    r.raise_for_status()
    return _decode_response(r.text)


def fetch_warn_messages(token: str, device: str, limit: int = 20) -> list[dict]:
    """Historical leak alerts for one sensor: list of {message, time, read}."""
    url = f"{GOVEE_BASE}/leak/rest/device/v1/warnMessage"
    headers = {**request_headers(token), "Content-Type": "application/json"}
    body = {"sku": "H5054", "device": device, "limit": limit}
    r = requests.post(url, headers=headers, json=body, timeout=15)
    if r.status_code == 401:
        raise PermissionError("warnMessage 401")
    r.raise_for_status()
    payload = _decode_response(r.text)
    return payload.get("data") or []


def _parse_one_sensor(d: dict) -> Sensor:
    ext = d.get("deviceExt", {}) or {}
    ds = json.loads(ext.get("deviceSettings") or "{}")
    ldd = json.loads(ext.get("lastDeviceData") or "{}")
    try:
        slot: int | None = int(ds.get("header")) if ds.get("header") not in (None, "") else None
    except (TypeError, ValueError):
        slot = None
    return Sensor(
        device=d["device"],
        name=d.get("deviceName") or d["device"],
        battery=ds.get("battery"),
        online=bool(ldd.get("online")),
        gwonline=bool(ldd.get("gwonline")),
        last_time=int(ldd.get("lastTime") or 0),
        firmware=str(d.get("versionSoft") or ds.get("versionSoft") or ""),
        slot=slot,
    )


def parse_sensors(devices_response: dict, log=None) -> list[Sensor]:
    out: list[Sensor] = []
    for d in devices_response.get("data", {}).get("devices", []):
        if d.get("sku") != "H5054":
            continue
        # One malformed device (bad JSON in deviceSettings/lastDeviceData, a
        # missing "device" key) must not drop every OTHER sensor's reading
        # for the whole poll cycle — isolate the parse per device.
        try:
            out.append(_parse_one_sensor(d))
        except Exception as e:
            if log is not None:
                log.warning("skipping malformed H5054 device %r: %s", d.get("device", "?"), e)
    return out


def _parse_one_gateway(g: dict) -> Gateway:
    ext = g.get("deviceExt", {}) or {}
    ds = json.loads(ext.get("deviceSettings") or "{}")
    ldd = json.loads(ext.get("lastDeviceData") or "{}")
    return Gateway(
        device=g["device"],
        name=g.get("deviceName") or "Govee Water Sensor Gateway",
        online=bool(ldd.get("online")),
        last_heartbeat=int(ldd.get("lastTime") or 0),
        firmware=str(g.get("versionSoft") or ds.get("versionSoft") or ""),
        mac=str(ds.get("mac") or ""),
    )


def parse_gateway(gateways_response: dict, log=None) -> Gateway | None:
    # Only one gateway is ever published (the first H5040 found), but a
    # malformed entry still shouldn't raise past a well-formed one earlier
    # or later in the list — try each in order, keep the first that parses.
    for g in gateways_response.get("data", {}).get("gateways", []):
        if g.get("sku") != "H5040":
            continue
        try:
            return _parse_one_gateway(g)
        except Exception as e:
            if log is not None:
                log.warning("skipping malformed H5040 gateway %r: %s", g.get("device", "?"), e)
    return None


def _sensor_device_block(s: Sensor, gw_id: str | None) -> dict:
    return build_device_block(
        identifiers=[f"govee_h5054_{s.device}"],
        name=s.name,
        manufacturer="Govee",
        model="H5054",
        sw_version=s.firmware or None,
        via_device=gw_id,
    )


def _gateway_device_block(g: Gateway, gw_id: str) -> dict:
    connections = [["mac", g.mac.lower()]] if g.mac else None
    return build_device_block(
        identifiers=[gw_id],
        name=g.name,
        manufacturer="Govee",
        model="H5040",
        sw_version=g.firmware or None,
        connections=connections,
    )


def discovery_specs(
    sensors: list[Sensor], gateway: Gateway | None
) -> list[tuple[str, str, dict]]:
    """Return (component, unique_id, payload) triples for HA MQTT discovery.

    ``unique_id`` is the slash-joined ``<device-uid>/<entity-slug>`` that
    HA accepts as the discovery topic's node-id/object-id pair — passed
    straight into ``ThreadedPublisher.publish_discovery``.
    """
    items: list[tuple[str, str, dict]] = []
    avail = availability_block(BRIDGE_LWT_TOPIC)
    gw_id = f"govee_h5040_{gateway.device}" if gateway else None

    for s in sensors:
        dev_uid = f"govee_h5054_{s.device}"
        device_block = _sensor_device_block(s, gw_id)

        sensor_entities = [
            # (component, slug, name, extra_kwargs)
            (
                "binary_sensor",
                "leak",
                "Leak",
                {
                    "device_class": "moisture",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                    "json_attributes_topic": f"{TOPIC_PREFIX}/{s.device}/leak/attrs",
                },
            ),
            (
                "sensor",
                "battery",
                "Battery",
                {
                    "device_class": "battery",
                    "unit_of_measurement": "%",
                    "state_class": "measurement",
                },
            ),
            (
                "binary_sensor",
                "online",
                "Online",
                {
                    "device_class": "connectivity",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                },
            ),
            (
                "sensor",
                "last_leak",
                "Last leak",
                {"device_class": "timestamp", "icon": "mdi:water-alert"},
            ),
            (
                "sensor",
                "firmware",
                "Firmware",
                {"entity_category": "diagnostic", "icon": "mdi:chip"},
            ),
            (
                "sensor",
                "slot",
                "Gateway slot",
                {"entity_category": "diagnostic", "icon": "mdi:numeric"},
            ),
        ]
        for component, slug, name, extras in sensor_entities:
            uid = f"{dev_uid}_{slug}"
            items.append(
                (
                    component,
                    f"{dev_uid}/{slug}",
                    build_discovery_payload(
                        name=name,
                        unique_id=uid,
                        object_id=uid,
                        state_topic=f"{TOPIC_PREFIX}/{s.device}/{slug}",
                        device=device_block,
                        **avail,
                        **extras,
                    ),
                )
            )

    if gateway and gw_id:
        device_block = _gateway_device_block(gateway, gw_id)
        gateway_entities = [
            (
                "binary_sensor",
                "online",
                "Online",
                {
                    "device_class": "connectivity",
                    "payload_on": "ON",
                    "payload_off": "OFF",
                },
            ),
            (
                "sensor",
                "last_heartbeat",
                "Last heartbeat",
                {"device_class": "timestamp", "icon": "mdi:heart-pulse"},
            ),
            (
                "sensor",
                "firmware",
                "Firmware",
                {"entity_category": "diagnostic", "icon": "mdi:chip"},
            ),
            (
                "sensor",
                "mac",
                "MAC",
                {"entity_category": "diagnostic", "icon": "mdi:network"},
            ),
        ]
        for component, slug, name, extras in gateway_entities:
            uid = f"{gw_id}_{slug}"
            items.append(
                (
                    component,
                    f"{gw_id}/{slug}",
                    build_discovery_payload(
                        name=name,
                        unique_id=uid,
                        object_id=uid,
                        state_topic=f"{TOPIC_PREFIX}/{gateway.device}/{slug}",
                        device=device_block,
                        **avail,
                        **extras,
                    ),
                )
            )

    return items


def publish_state(
    pub: ThreadedPublisher,
    sensors: list[Sensor],
    gateway: Gateway | None,
    leak_until: dict[str, int],
) -> None:
    t = now_ms()
    for s in sensors:
        base = f"{TOPIC_PREFIX}/{s.device}"
        leak = "ON" if leak_until.get(s.device, 0) > t else "OFF"
        pub.publish_state(f"{base}/leak", leak)
        if s.battery is not None:
            pub.publish_state(f"{base}/battery", str(s.battery))
        pub.publish_state(f"{base}/online", "ON" if s.online else "OFF")
        last_leak_iso = epoch_ms_to_iso(s.last_time)
        if last_leak_iso:
            pub.publish_state(f"{base}/last_leak", last_leak_iso)
        pub.publish_state(f"{base}/firmware", s.firmware or "")
        pub.publish_state(f"{base}/slot", "" if s.slot is None else str(s.slot))
    if gateway:
        base = f"{TOPIC_PREFIX}/{gateway.device}"
        pub.publish_state(f"{base}/online", "ON" if gateway.online else "OFF")
        last_heartbeat_iso = epoch_ms_to_iso(gateway.last_heartbeat)
        if last_heartbeat_iso:
            pub.publish_state(f"{base}/last_heartbeat", last_heartbeat_iso)
        pub.publish_state(f"{base}/firmware", gateway.firmware or "")
        pub.publish_state(f"{base}/mac", gateway.mac or "")


def publish_history(pub: ThreadedPublisher, device: str, alerts: list[dict]) -> None:
    """Publish a sensor's leak-alert history as a JSON attribute on the leak entity.

    Alerts in the response come unsorted; we sort newest-first and convert
    epoch ms to ISO. Govee's `message` field uses U+00A0 (non-breaking space)
    in its boilerplate — we collapse it to a regular space for readability.
    """
    cleaned = []
    for a in alerts:
        try:
            t_ms = int(a.get("time") or 0)
        except (TypeError, ValueError):
            t_ms = 0
        cleaned.append({
            "time": epoch_ms_to_iso(t_ms),
            "time_ms": t_ms,
            "message": (a.get("message") or "").replace("\u00a0", " "),
            "read": bool(a.get("read")),
        })
    cleaned.sort(key=lambda x: x["time_ms"], reverse=True)
    payload = {"history": cleaned, "count": len(cleaned)}
    pub.publish_attributes(f"{TOPIC_PREFIX}/{device}/leak/attrs", payload)


def _baseline_topic(device: str) -> str:
    # Internal bookkeeping topic, not part of HA discovery — retained so
    # the bridge can read its own last-known `lastTime` back on restart.
    return f"{TOPIC_PREFIX}/{device}/_leak_baseline_ms"


def seed_baseline_from_retained(
    pub: ThreadedPublisher,
    sensors: list[Sensor],
    log,
    wait_s: float = 2.5,
) -> dict[str, int]:
    """Return the starting `baseline_last_time` map, preferring each
    sensor's own persisted (retained MQTT) `lastTime` over its freshly
    fetched current value.

    Without this, every bridge restart re-baselines against whatever
    `lastTime` the Govee API reports RIGHT NOW — which already reflects
    any leak that happened while the bridge was down, so that leak can
    never register as "new" (baseline == current, not strictly less).
    A leak during downtime was silently absorbed into the post-restart
    baseline. Reading back the last value THIS bridge itself observed
    (persisted as a retained topic — this stack has no data volume, same
    approach the Emporia Vue bridge's `seed_energy_from_retained` uses)
    closes that gap: if Govee's API now reports a `lastTime` newer than
    what we last saw before going down, the normal "current > baseline"
    detection in the main loop fires immediately on the first post-
    restart poll.

    Still not perfect — see README's Limitations section: this is a
    single-value watermark, not a timeline, so it can't distinguish one
    leak from several that occurred during the same downtime window
    (the `history` attribute, refreshed whenever a leak IS detected,
    covers that up to its own limit of 20 alerts). And on a first-ever
    run (nothing retained yet), or if the broker has no persistence and
    lost its retained state, there's nothing to seed from — this
    function falls back to each sensor's current `lastTime`, i.e.
    today's baseline behavior, which is a real remaining edge case.
    """
    baseline: dict[str, int] = {s.device: s.last_time for s in sensors}
    if not sensors:
        return baseline

    by_topic = {_baseline_topic(s.device): s.device for s in sensors}

    def _on_retained(topic: str, payload: bytes) -> None:
        device = by_topic.get(topic)
        if device is None:
            return
        try:
            persisted = int(payload.decode())
        except (ValueError, UnicodeDecodeError):
            log.warning("bad retained leak baseline payload on %s: %r", topic, payload)
            return
        # Never move the baseline BACKWARD relative to the value we just
        # fetched from the API — only forward-fill from a lower fresh
        # value up to a higher persisted one (the case this exists for).
        if persisted > baseline.get(device, 0):
            baseline[device] = persisted

    for topic in by_topic:
        pub.subscribe(topic, _on_retained)
    if not pub.wait_until_connected(timeout=10.0):
        log.warning("MQTT not connected after 10s; leak baseline may start from current API state only")
    time.sleep(wait_s)
    noop = lambda _topic, _payload: None  # noqa: E731
    for topic in by_topic:
        pub.subscribe(topic, noop)
    return baseline


def publish_baseline(pub: ThreadedPublisher, baseline_last_time: dict[str, int]) -> None:
    """Persist the current baseline as a retained topic per sensor, so
    the NEXT restart can read it back via `seed_baseline_from_retained`."""
    for device, last_time in baseline_last_time.items():
        pub.publish_raw(_baseline_topic(device), str(last_time), retain=True)


def main() -> int:
    log = configure_logging("govee-mqtt-bridge", LOG_LEVEL)
    log.info("starting; client_id=%s poll=%ss leak_hold=%ss", CLIENT_ID, POLL_INTERVAL, LEAK_HOLD_SECONDS)

    token, token_expires_at = login()
    log.info("login ok; token expires in %ds", (token_expires_at - now_ms()) // 1000)

    pub = ThreadedPublisher(
        host=MQTT_HOST,
        port=MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASS,
        client_id=f"govee-mqtt-bridge-{uuid.uuid4().hex[:8]}",
        lwt_topic=BRIDGE_LWT_TOPIC,
        discovery_prefix=DISCOVERY_PREFIX,
        health_path="/tmp/healthy",
        tls=MQTT_TLS,
        ca_file=MQTT_CA_FILE,
    )
    pub.start()

    baseline_last_time: dict[str, int] = {}
    leak_until: dict[str, int] = {}
    discovery_published = False
    backfill_done = False

    stopping = False

    def on_signal(signum, _frame):
        nonlocal stopping
        log.info("signal %s, shutting down", signum)
        stopping = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    def _on_ha_birth() -> None:
        # Retained discovery configs usually survive an HA restart, but
        # not always (a broker restart with no persistence, a manual
        # "purge retained messages") — re-publishing on HA's birth
        # message closes that gap without a bridge restart.
        nonlocal discovery_published
        log.info("HA birth message received; re-publishing discovery")
        discovery_published = False

    watch_ha_birth(pub, _on_ha_birth, discovery_prefix=DISCOVERY_PREFIX)

    while not stopping:
        try:
            # Refresh token a minute before expiry.
            if now_ms() > token_expires_at - 60_000:
                token, token_expires_at = login()
                log.info("token refreshed; expires in %ds", (token_expires_at - now_ms()) // 1000)

            devices = fetch_devices(token)
            gateways = fetch_gateways(token)
            sensors = parse_sensors(devices, log)
            gateway = parse_gateway(gateways, log)

            if not discovery_published:
                if not sensors:
                    log.warning("no H5054 sensors found in account; check Govee app")
                for component, unique_id, payload in discovery_specs(sensors, gateway):
                    pub.publish_discovery(
                        component=component,
                        unique_id=unique_id,
                        payload=payload,
                    )
                # The ThreadedPublisher's LWT machinery publishes "online" on
                # connect automatically; no manual publish needed here.
                discovery_published = True
                log.info(
                    "discovery published: %d sensors, gateway=%s",
                    len(sensors),
                    gateway.device if gateway else "none",
                )

            if not backfill_done:
                # Prefer each sensor's persisted (retained-MQTT) lastTime
                # over its freshly fetched current value, so a leak that
                # happened while the bridge was down still registers as
                # "new" on this first post-restart poll — see
                # `seed_baseline_from_retained`. Independent of
                # discovery_published so an HA-birth re-publish of
                # discovery doesn't also re-seed the baseline or re-run
                # the history backfill below.
                baseline_last_time = seed_baseline_from_retained(pub, sensors, log)
                publish_baseline(pub, baseline_last_time)
                # Initial history backfill — one warnMessage call per sensor.
                # Subsequent refreshes happen only on new-leak detection below.
                for s in sensors:
                    try:
                        alerts = fetch_warn_messages(token, s.device, limit=20)
                        publish_history(pub, s.device, alerts)
                    except Exception as e:
                        log.warning("history backfill failed for %s: %s", s.device, e)
                backfill_done = True

            for s in sensors:
                if s.last_time > baseline_last_time.get(s.device, 0):
                    log.warning("LEAK %s (%s) lastTime=%d", s.name, s.device, s.last_time)
                    leak_until[s.device] = now_ms() + LEAK_HOLD_SECONDS * 1000
                    baseline_last_time[s.device] = s.last_time
                    # Persist immediately (not just at startup) so the
                    # retained baseline this sensor reads back on its
                    # NEXT restart reflects this leak, not a stale one.
                    pub.publish_raw(_baseline_topic(s.device), str(s.last_time), retain=True)
                    # Refresh just this sensor's history so the new alert appears
                    # in HA's attribute list.
                    try:
                        alerts = fetch_warn_messages(token, s.device, limit=20)
                        publish_history(pub, s.device, alerts)
                    except Exception as e:
                        log.warning("history refresh failed for %s: %s", s.device, e)

            publish_state(pub, sensors, gateway, leak_until)

        except PermissionError:
            log.warning("auth expired mid-poll; re-logging in")
            try:
                token, token_expires_at = login()
            except Exception as e:
                log.error("re-login failed: %s", e)
                time.sleep(30)
        except requests.RequestException as e:
            log.error("network/HTTP error: %s", e)
        except Exception:
            log.exception("poll failed")

        for _ in range(POLL_INTERVAL):
            if stopping:
                break
            time.sleep(1)

    pub.stop()
    log.info("shutdown clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())
