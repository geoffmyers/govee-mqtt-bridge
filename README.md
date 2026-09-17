<p align="center">
  <img src="docs/icon.svg" width="96" height="96" alt="Govee MQTT Bridge icon">
</p>

# Govee MQTT Bridge

<!-- BADGES:START -->
![Python 3.12](https://img.shields.io/badge/Python-3.12-3776ab?style=flat-square&logo=python)
[![Container image](https://img.shields.io/badge/ghcr.io-geoffmyers%2Fgovee--mqtt--bridge-2496ED?style=flat-square&logo=docker&logoColor=white)](https://github.com/geoffmyers/govee-mqtt-bridge/pkgs/container/govee-mqtt-bridge)
[![Licence GPL-3.0-or-later](https://img.shields.io/badge/licence-GPL--3.0--or--later-blue?style=flat-square)](LICENSE.md)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=flat-square)](CONTRIBUTING.md)
<!-- BADGES:END -->

## Table of Contents

- [Description](#description)
- [Features](#features)
- [Requirements](#requirements)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Architecture](#architecture)
- [Credits](#credits)
- [Contributing](#contributing)
- [License](#license)

## Description

A small Python daemon that polls Govee's cloud account API — the same one the
iOS app uses — for the state of a **Govee H5040** Wi-Fi gateway and its paired
**H5054** water leak sensors, and republishes that state over MQTT with Home
Assistant [MQTT Discovery](https://www.home-assistant.io/integrations/mqtt/#mqtt-discovery),
so the sensors show up in Home Assistant automatically.

Govee's official *Developer* API (the one that issues an API key) does not
expose H5054 leak sensors at all. Every published Home Assistant integration
for the H5054 that we could find works around this by decoding the 433 MHz
radio link between sensor and gateway with an SDR dongle and `rtl_433`. This
bridge takes a different path: it polls the cloud endpoints the gateway
already reports to, so no SDR, antenna or gain tuning is needed. The
trade-off is detection latency equal to the poll interval (30 seconds by
default) instead of near-instant radio decode.

## Features

- Polls Govee's cloud account API for gateway and leak-sensor state — no SDR
  hardware required.
- Publishes Home Assistant MQTT Discovery configs automatically; devices and
  entities appear in Home Assistant with no manual YAML.
- Leak detection with a configurable hold time, so a brief alert survives a
  missed poll cycle instead of clearing instantly.
- Publishes each sensor's recent leak-alert history as an entity attribute.
- Handles Govee's JWT session lifetime automatically, including proactive
  and reactive re-login.
- Multiple sensors and one gateway auto-discover on the next poll after you
  pair them in the Govee app — no bridge config change needed.

## Requirements

- **Docker** with the Compose plugin (Compose v2.17+, for `additional_contexts`)
- An MQTT broker reachable from the container, with Home Assistant's MQTT
  integration pointed at the same broker
- A Govee account with an **H5040** Wi-Fi gateway and one or more **H5054**
  water leak sensors already paired in the Govee app
- A `clientId` already registered with your Govee account (see
  [Configuration](#configuration) below) — extracted once from your phone's
  Govee app

## Installation

```bash
git clone https://github.com/geoffmyers/govee-mqtt-bridge.git
cd govee-mqtt-bridge

cp .env.example .env
cp docker-compose.example.yml docker-compose.yml
```

Edit `.env` with your Govee credentials, `clientId` and MQTT broker details
(see [Configuration](#configuration)), then build and start the bridge:

```bash
docker compose build
docker compose up -d
docker compose logs -f
```

The application code is bind-mounted from `./app` (see
`docker-compose.example.yml`), so after the first build a code change only
needs a container restart, not a rebuild.

The image is also published on the GitHub Container Registry as
`ghcr.io/geoffmyers/govee-mqtt-bridge`, for `linux/amd64` and `linux/arm64`, with the
application code in it: `docker compose pull` fetches it instead of
building. The compose file still mounts `./app` over that copy, so the
code in your checkout is what runs.

## Usage

Once running, the bridge logs in, discovers your H5040 gateway and H5054
sensors, publishes their Home Assistant Discovery configs, and starts
polling. Each device appears in Home Assistant as its own device:

### MQTT entities published

**Per H5054 leak sensor** (6 entities):

| Entity | Component | Notes |
|---|---|---|
| Leak | `binary_sensor` | `device_class: moisture`; carries a `history` attribute with the sensor's last 20 leak alerts |
| Battery | `sensor` | `device_class: battery`, % |
| Online | `binary_sensor` | `device_class: connectivity` |
| Last leak | `sensor` | `device_class: timestamp`; the most recent leak alert, or unknown if it has never fired |
| Firmware | `sensor` | diagnostic |
| Gateway slot | `sensor` | diagnostic; the numbered slot on the H5040 the sensor is paired to |

**Per H5040 gateway** (4 entities): Online (`binary_sensor`, connectivity),
Last heartbeat (`sensor`, timestamp), Firmware (`sensor`, diagnostic), MAC
(`sensor`, diagnostic).

Renaming a sensor in the Govee app updates its Home Assistant name on the
next poll; the entity `unique_id` is keyed on the device ID, not the name, so
history is preserved. Pairing a new sensor in the Govee app makes it appear
in Home Assistant on the next poll cycle with no bridge restart needed.

If Govee changes an endpoint's shape, expect log errors and entities going
`unavailable` rather than a crash.

## Configuration

Environment variables, set in `.env` (`.env.example` lists them all):

| Variable | Default | Description |
|---|---|---|
| `GOVEE_EMAIL` | *(required)* | Govee account email |
| `GOVEE_PASSWORD` | *(required)* | Govee account password |
| `GOVEE_CLIENT_ID` | *(required)* | A `clientId` already registered with your account — see below |
| `MQTT_HOST` | `mosquitto` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | *(empty)* | MQTT username |
| `MQTT_PASSWORD` | *(required)* | MQTT password |
| `POLL_INTERVAL` | `30` | Seconds between polls |
| `GOVEE_APP_VERSION` | `7.4.21` | App version sent to Govee's API; raise it if Govee starts rejecting an old one |
| `GOVEE_USER_AGENT` | the Govee Home iOS app's | User-Agent sent to Govee's API |
| `LEAK_HOLD_SECONDS` | `600` | How long the leak entity stays `ON` after a detected leak |
| `HA_DISCOVERY_PREFIX` | `homeassistant` | Home Assistant MQTT Discovery topic prefix |
| `MQTT_TOPIC_PREFIX` | `govee/leak` | Prefix for this bridge's own MQTT topics |
| `LOG_LEVEL` | `INFO` | Python log level |
| `GITHUB_ERROR_TOKEN` | *(unset)* | Optional. A GitHub token with `repo` scope; when set together with `GITHUB_REPO`, uncaught exceptions are filed as GitHub issues via `repository_dispatch` (see [Credits](#credits)) |
| `GITHUB_REPO` | *(unset)* | Optional. `owner/name` of the repo to file error reports against |
| `GITHUB_ERROR_ENVIRONMENT` | `production` | Optional. Environment label attached to filed error reports |

### The `clientId` requirement

Govee silently rejects any `clientId` that hasn't already been registered
with your account — the login endpoint returns `{"status":454,"message":""}`
with no further explanation. The iOS app registers its `clientId` at first
install through a flow this bridge doesn't reproduce, so the practical
workaround is to reuse the `clientId` your phone's Govee app already uses.
It's an installation identifier, not a session secret, so the bridge and
your phone can be logged in at the same time.

To find it:

1. Run a debugging proxy (for example [Proxyman](https://proxyman.io/)) with
   its CA certificate installed on your phone.
2. Open the Govee app and let it make any authenticated request.
3. Read the `clientId` header from that request (a 32-character hex string).
4. Put it in `.env` as `GOVEE_CLIENT_ID`.

If your phone's `clientId` ever changes (for example, you reinstall the
Govee app), update `.env` and restart the bridge.

## Architecture

```
Govee cloud API  ◄──poll──  govee-mqtt-bridge  ──publish──►  MQTT broker  ──►  Home Assistant
(app2.govee.com)             (Python, paho-mqtt)              (mosquitto)      (MQTT Discovery)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the endpoint list, leak-detection
logic and module layout.

## Credits

- MQTT client, Home Assistant Discovery payloads and topic/timestamp helpers
  come from this repository's own `ha-mqtt-bridge-toolkit` package, vendored
  in at `_shared/ha-mqtt-bridge-toolkit/` when this repository is published.
- Optional production-error reporting uses this repository's own
  `python-github-error-reporter` package, vendored in the same way at
  `_shared/python-github-error-reporter/`.
- The Govee cloud auth flow (login endpoint, header set, request body, JWT
  handling) was worked out by reading
  [`wez/govee2mqtt`](https://github.com/wez/govee2mqtt)'s `undoc_api.rs`; the
  sensor-read endpoint was found by capturing the iOS Govee app's own traffic.
  These are **undocumented, reverse-engineered endpoints**, not an official
  Govee integration API, and may change or break without notice.
- The README icon is the [Font Awesome](https://fontawesome.com/) `droplet`
  glyph, used under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
- This project is not affiliated with, endorsed by, or supported by Govee.

Written by Geoff Myers.

## Contributing

Bug reports and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md)
for setup, checks and how this repository is published.

## License

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See [LICENSE.md](LICENSE.md) for the full text of the GNU
General Public License.

SPDX-License-Identifier: `GPL-3.0-or-later`
