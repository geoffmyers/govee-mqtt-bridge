# Architecture

A single Python process that logs into Govee's app-facing (undocumented)
account API, polls it on a fixed interval, and republishes what it finds as
Home Assistant MQTT Discovery entities plus per-poll state updates.

```
Govee cloud (app2.govee.com)
   │  login → JWT
   │  device/list, gateways, warnMessage
   ▼
main.py  ── ThreadedPublisher (ha_mqtt_bridge) ──►  MQTT broker
                                                        │
                                                        ▼
                                                  Home Assistant
                                                  (MQTT Discovery)
```

## Layout

| Path | Role |
|---|---|
| `app/main.py` | Everything: login/token refresh, polling, leak-event detection, HA Discovery payload assembly, and the main loop. |
| `app/test_discovery_regression.py` | Pins the exact discovery topics and JSON payloads the bridge emits, against a fixed sensor + gateway fixture, so a refactor cannot silently change what Home Assistant sees. |
| `app/requirements.txt` | `paho-mqtt`, `requests`. |
| `Dockerfile` | Installs the vendored `ha_mqtt_bridge` and `python_github_error_reporter` packages, then the app's own requirements. The application code itself is bind-mounted at runtime, not baked into the image, so a code change needs only a container restart. |
| `docker-compose.example.yml` | Builds the image with the two packages wired in via Compose `additional_contexts` pointing at `./_shared/...`. |

## Shared code

Two packages are vendored into this repository under `_shared/` at publish
time (see the mono repo's publishing tooling) rather than maintained twice:

- **`ha_mqtt_bridge`** — the MQTT client (`ThreadedPublisher`: connect, LWT,
  reconnect, five publish flavors), HA Discovery payload construction,
  device-block assembly, and epoch→ISO-8601 timestamp formatting. Shared with
  the other MQTT bridges in this family.
- **`python_github_error_reporter`** — an opt-in uncaught-exception reporter.
  It installs `sys.excepthook` and `threading.excepthook`; when
  `GITHUB_ERROR_TOKEN` and `GITHUB_REPO` are both set it dispatches a
  `repository_dispatch` event on the first occurrence of a given error type,
  then silently deduplicates repeats for 60 seconds. With neither variable
  set (the default) it does nothing.

## Data flow

1. **Login** — `POST /account/rest/account/v1/login` with the account email,
   password and a registered `clientId`; the response carries a bearer JWT
   good for roughly 12 hours. The bridge re-logs in 60 seconds before expiry,
   and immediately on any 401 mid-poll.
2. **Poll** (`POLL_INTERVAL`, default 30 s) — `GET /bff-app/v1/device/list`
   for every `H5054` sensor's battery/online/last-leak state, and
   `GET /app/v2/gateways` for the `H5040` gateway's online state and
   heartbeat. Both responses are sometimes base64-wrapped JSON; the bridge
   handles both forms transparently.
3. **Leak detection** — each sensor carries `lastDeviceData.lastTime`, the
   epoch-ms of its most recent leak alert (`0` if never triggered). On
   startup the bridge baselines this value per sensor; on each later poll a
   strictly-greater value means a new leak event fired since the last check.
   The leak `binary_sensor` is held `ON` for `LEAK_HOLD_SECONDS` (default 600)
   after detection, then clears automatically — long enough that a brief
   alert survives Home Assistant being offline for part of the hold window.
4. **History backfill** — `POST /leak/rest/device/v1/warnMessage` returns up
   to 20 past alerts per sensor. It is called once per sensor at startup, and
   again for a sensor whenever its `lastTime` advances, keeping the
   steady-state cost at two calls per poll cycle regardless of sensor count.
5. **Discovery** — published once, on the first successful poll after
   startup: one HA device per `H5054` sensor (`via_device` linking it to its
   `H5040` gateway) and one device for the gateway itself.

## Entities published

See the README's [MQTT entities published](README.md#mqtt-entities-published)
table for the full per-sensor and per-gateway entity list.
