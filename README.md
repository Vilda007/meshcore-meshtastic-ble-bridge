# MeshCore ↔ Meshtastic BLE Bridge

A self-contained Bluetooth Low Energy bridge between two LoRa mesh networks:
[MeshCore](https://github.com/ripplebiz/MeshCore) and
[Meshtastic](https://github.com/meshtastic/firmware). It runs on a single
Raspberry Pi and relays text messages between channel pairs of both meshes —
no cables, no WiFi, no MQTT broker needed: **both radios are connected purely
over BLE**.

```
 MeshCore node          Raspberry Pi             Meshtastic node
 (companion radio)      ── bridge.py ──           (any BLE-capable firmware)
      ⇄ BLE ⇄ ────────▶ forward + "MC: " ────▶  channel per mapping
                         forward + "MT: " ◀─────   channel per mapping
```

Messages received on a bridged MeshCore channel are re-sent on the paired
Meshtastic channel (prefixed `MC: `), and vice versa (prefixed `MT: `).
DMs are never bridged.

> **Reference deployment:** this bridge is developed and continuously tested
> on a live installation in **Brno, Czech Republic**, running on compact
> indoor nodes with *(very)* limited radio range. Most of the traffic you
> may see on the bridged channels is Czech chat and test messages — don't
> be surprised, and don't expect to reach the test nodes from afar. The
> bridge's range depends entirely on the radios you attach to it.

## Highlights

- **Pure BLE on both ends** — the radios stay portable and cable-free; the Pi
  only needs its onboard Bluetooth adapter.
- **Bijective channel mapping** — one MeshCore channel ↔ one Meshtastic
  channel per pair; multiple pairs allowed. Editable live from the web UI
  (picked up within ~15 s, no restart).
- **Loop protection** — messages already carrying the `MC:`/`MT:` prefixes
  are never re-forwarded; LRU dedup cache filters duplicates.
- **Offline queues** — if the other side is down, messages wait in a bounded
  queue (50) and are flushed automatically after reconnect. Nothing is lost
  during outages.
- **Auto-reconnect with hardened timeouts** — a known failure mode of the
  `meshtastic` python library is a silently dead GATT connection
  (`connection.lost` never fires) where `sendText`/`setTime` block forever.
  Every blocking BLE call here runs under a timeout; on expiry the link is
  marked broken, messages queue up and a reconnect is forced in the
  background.
- **Chunking** — long messages are split to the receiver's limit
  (MeshCore ~133 chars, Meshtastic ~233) as `[1/2]`, `[2/2]`, …
- **Delivery confirmation on the MeshCore side** (à la hoplink): the
  firmware's group-message encryption is deterministic AES-128-ECB
  (no IV), so the bridge can predict the exact ciphertext it sends.
  The companion radio streams every raw received packet
  (`PACKET_LOG_DATA` sniffer); when a repeater re-transmits our message,
  the bridge matches the payload byte-for-byte and logs a **repeater
  echo** with SNR/RSSI. If no echo arrives within `mc_confirm_window`,
  TX counters (`CMD_GET_STATS`) still tell whether the radio transmitted
  at all. Works only when the repeater's signal reaches our node.
- **Watchdog hardening**: the service runs as `Type=notify` with
  `WatchdogSec=180` — the bridge pings systemd and, if the event loop
  hangs (or a health self-check fails), systemd restarts it. The health
  check fails when the daily maintenance has not completed within
  `maintenance_interval_hours + health_grace_minutes`.
- **Web dashboard** (port 8420, zero dependencies): login-protected UI in
  **English or Czech** (flag toggle, default EN, cookie-stored) with —
  - live status cards and the message/event feed with filters,
    **full-text search and paging** (25 records/page, served from the
    SQLite history)
  - daily **statistics charts** (forwarded messages, reconnects, uptime %)
  - the **channel-mapping editor** and a **power switch** for the bridge
    service
  - a **Config page** — the whole configuration lives in SQLite
    (`stats.db`, layered over the `config.ini` defaults), editable from
    the browser; the bridge picks changes up within ~15 s (items marked
    ↻ need a service restart)
  - **user management** — login/logout, user accounts with PBKDF2-hashed
    passwords, 30-day sessions; regular users are read-only viewers,
    admins can edit config/mapping/power/users
- **Persistent statistics** (SQLite, `stats.db`): every forwarded message
  and event is stored; the dashboard serves a searchable, paginated feed
  (`/api/feed`) and daily aggregates (`/api/history`): messages/day per
  direction, reconnects/day and connected-time % per side (computed from
  connect/disconnect events, bridge start/stop boundaries included).
- **MQTT side** (optional, `paho-mqtt`): feeds home-automation and web
  clients — publishes every received mesh message, bridge status
  (retained, every 60 s) and **Home Assistant MQTT discovery**, so
  bridge entities (connectivity, forwarded counters, queues, last
  messages) appear in HA automatically. Optional inbound topic posts
  MQTT messages into the mesh. See [MQTT](#mqtt).
- **Rate limiting** — per-direction minimum interval between forwarded
  messages (`rate_limit_*`, 0 = off), protecting the LoRa channel from
  floods and bursts; applies to live traffic and queue flushes.
- **Daily maintenance** — clock sync of both radios, pruning of stale
  MeshCore contacts and Meshtastic nodeDB entries (favorites are never
  removed), with a per-run removal limit and a dry-run mode.
- **Stuck-GATT self-healing** — failed handshakes are cleaned up via
  `bluetoothctl disconnect` so the device advertises again.

## How it works

- **MeshCore side** (`meshcore_ble.py`): speaks the companion-radio BLE
  protocol (service `6E400001-B5A3-F393-E0A9-E50E24DCCA9E`) directly over
  [bleak](https://github.com/hbldh/bleak): push notifications, message
  polling (`CMD_SYNC_NEXT_MESSAGE`), channel messages
  (`CMD_SEND_CHANNEL_TXT_MSG`), clock sync, contacts and channel management.
- **Meshtastic side** (`bridge.py`): the official
  [`meshtastic`](https://github.com/meshtastic/python) python library
  (`BLEInterface`), receiving via pubsub (`meshtastic.receive`), sending via
  `sendText`, admin operations via the node API.
- **Startup order matters**: concurrent BLE scans of the MeshCore client and
  the meshtastic library on one adapter break each other's handshake, so the
  bridge connects Meshtastic first, MeshCore second (staggered start).
- **Timestamps**: both device clocks are set from the Pi after connect and
  once a day (MeshCore only when the drift exceeds 30 s — its firmware
  refuses time going backwards).

## Requirements

**Hardware**

- Raspberry Pi (or any Debian machine with Bluetooth) within BLE range of
  both radios
- A MeshCore node flashed with the **companion radio** firmware
- A Meshtastic node with Bluetooth enabled **and light sleep disabled**
  (`setup_tbeam.py` does this for you — see
  [Troubleshooting](#troubleshooting--gotchas))

**Software**

- Raspberry Pi OS / Debian with working BlueZ
- Python 3.9+ (everything else is installed by `install.sh` into a venv:
  `bleak`, `meshtastic`, `dbus-fast`, `pubsub`, `pycryptodome` — the last
  one is optional, only needed for exact repeat-echo confirmation)

## Installation

> All commands run on the Pi unless noted. The bridge directory below is
> assumed to be `~/mesh-bridge` — adjust to taste.

1. **Get the code**

   ```bash
   git clone https://github.com/Vilda007/meshcore-meshtastic-ble-bridge.git
   cd meshcore-meshtastic-ble-bridge
   ```

2. **Install** (creates a venv, installs libraries, registers systemd units)

   ```bash
   bash install.sh
   ```

3. **Configure** — copy the example and fill in your values:

   ```bash
   cp config.ini.example config.ini
   nano config.ini        # device names, channel mapping, channel key
   ```

   Everything marked `REPLACE_WITH_…` must be set — see the
   [Configuration](#configuration) reference below.

4. **Pair the radios** (one-time, needs the devices advertising; use the PINs
   configured in the companion app / Meshtastic app):

   ```bash
   venv/bin/python pair_bt.py <MAC_MESHCORE> <PIN_MESHCORE> <MAC_MESHTASTIC> <PIN_MESHTASTIC>
   ```

   Find the MACs with `bluetoothctl scan on` — look for your MeshCore
   companion (service `6E400001-…`) and your Meshtastic node.
   A Meshtastic node in light sleep does not advertise — reset it or press
   a button to wake it.

5. **(Optional) Create a shared channel** on both devices — see
   [Shared channel](#shared-channel-meshbridge).

6. **Test run** — start it in the foreground, send a message from the phone
   app and watch the log:

   ```bash
   venv/bin/python bridge.py
   ```

7. **Enable the services**

   ```bash
   sudo systemctl start mesh-bridge mesh-dashboard
   sudo systemctl enable mesh-bridge mesh-dashboard
   journalctl -u mesh-bridge -f        # live log
   ```

The dashboard is now at `http://<pi-ip>:8420/`.

## Configuration

Configuration has **three layers** (lower overrides higher):

1. **Code defaults** — sensible values baked into `bridge.py`
2. **`config.ini`** — bootstrap; copy from `config.ini.example` and edit the
   `REPLACE_WITH_…` values. Parsed at every start.
3. **SQLite (`stats.db`, `config` table)** — editable from the dashboard
   **Config** page (admin only); the bridge picks changes up within ~15 s.
   `config.ini` remains the bootstrap and the source of defaults — a fresh
   `stats.db` behaves exactly like the ini.

| Section | Option | Default | Meaning |
|---|---|---|---|
| `[meshcore]` | `device` | *(first found)* | Substring of the MeshCore BLE name (↻) |
| | `channel` | `0` | Default MC channel (fallback, no `[channels]` mappings) |
| | `poll_interval` | `30` | Seconds between polls / keep-alives |
| `[meshtastic]` | `device` | *(first found)* | Meshtastic BLE name or MAC (↻) |
| | `channel` | `0` | Default MT channel (fallback) |
| `[channels]` | `mappings` | `0:0` | Bridged pairs `mc:mt, …` — edited from the dashboard (stored in SQLite) |
| | `mc_channel_name` | `#meshbridge` | MC name of the shared channel (lowercase only) |
| | `mc_channel_key` | *(required)* | **16-byte hex** MeshCore channel key — a SECRET, never commit |
| | `mt_channel_name` | `#meshbridge` | MT channel name |
| | `mt_channel_psk` | `AQ==` | MT PSK; `AQ==` = default public key (joinable by name) |
| `[bridge]` | `prefix_to_mt` / `prefix_to_mc` | `MC:` / `MT:` | Forwarding prefixes (loop guard) |
| | `mc_max_len` / `mt_max_len` | `120` / `200` | Max body length per side |
| | `rate_limit_mc_to_mt` / `rate_limit_mt_to_mc` | `0` / `0` | Min seconds between two forwarded messages per direction (0 = off) |
| | `log_retention_days` | `0` | Auto-prune feed/statistics entries older than this many days, once a day with the maintenance (0 = keep forever) |
| | `dedup_cache` | `400` | LRU dedup cache entries |
| | `log_level` | `INFO` | Logging verbosity |
| | `event_log` / `status_file` | `events.jsonl` / `status.json` | Dashboard data files (↻) |
| | `mc_confirm_window` | `30` | Repeat-echo window for MeshCore delivery confirmation (s; 0 = off) |
| | `db_file` | `stats.db` | SQLite persistence: feed, statistics, config, users (↻) |
| `[maintenance]` | `mc_contact_max_age_days` | `14` | Prune MC contacts older than this |
| | `mt_node_max_age_days` | `7` | Prune MT nodeDB nodes older than this |
| | `maintenance_interval_hours` | `24` | Maintenance period |
| | `max_removals_per_run` | `10` | Removal safety limit |
| | `health_grace_minutes` | `90` | Health self-check grace over the maintenance period |
| | `dry_run` | `false` | Log only, delete nothing |
| `[mqtt]` | `enabled` | `false` | MQTT client on/off (paho-mqtt) |
| | `host` / `port` | *(none)* / `1883` | MQTT broker address |
| | `user` / `password` | *(none)* | Broker credentials (password admin-only in the UI) |
| | `topic_prefix` | `meshbridge` | Prefix for all bridge topics |
| | `allow_inbound` | `false` | Accept `<prefix>/send` messages **into the mesh** |
| | `hass_discovery` | `true` | Publish HA MQTT discovery (entities auto-appear) |

Items marked **↻** apply only after a service restart (the Config page
marks them the same way).

**What you must replace before use** (all gitignored — never commit):

- `config.ini` — your device names, your **channel key**
  (also editable later on the Config page)
- `deploy.local` (optional) — your SSH alias for `deploy.sh`
- the **dashboard admin password** (default `admin`/`admin`, see above)

## Shared channel (#meshbridge)

MeshCore has no "public channels by name" — a channel *is* its 16-byte key
(`SHA256(key)` = channel hash). The Meshtastic convention is the opposite:
a channel is public when it uses the default PSK (`AQ==`), joinable by name.
So a shared channel needs a different setup on each side.

The reference installation bridges a shared channel named **`#meshbridge`**:

| Side | Channel setup |
|---|---|
| **MeshCore** | Keyed channel: lowercase name `#meshbridge` + its own generated 16-byte key (the `mc_channel_key` setting). The key **is** the channel and is a *secret* — the test bridge's key is intentionally not published; if you want to join the test channel, ask the maintainer. (MeshCore channel names must be lowercase.) |
| **Meshtastic** | Public by convention: a secondary channel named `#meshbridge` with the default PSK `AQ==` — anyone can join simply by adding a channel with the same name and default key. |

For your own installation, generate your own MeshCore channel key (e.g.
`python3 -c "import os; print(os.urandom(16).hex())"`), put the name/key/PSK
in the `channels.*` settings, and let `create_channel.py` set the channel up
on both devices (idempotent; it reads everything from the configuration):

```bash
sudo systemctl stop mesh-bridge
venv/bin/python create_channel.py all
sudo systemctl start mesh-bridge
```

Then map the two channels in the dashboard (it prints the indices, e.g.
`mappings.json: [[2, 1]]`).

> **Sharing**: to let someone into the MeshCore side you must share the key
> itself (the app supports invites). The Meshtastic side is joinable by name
> alone. Only share with people you want in the channel.

## Dashboard

`http://<pi-ip>:8420/` — served by `dashboard.py` (stdlib only, no
dependencies), systemd unit `mesh-dashboard`.

**Authentication**: the dashboard requires login (users live in `stats.db`,
passwords are PBKDF2-hashed, sessions are 30-day `HttpOnly` cookies).
On the very first start a default admin **`admin`/`admin`** is created —
**change the password immediately** (Users page → *Change my password*;
the UI shows a warning banner until then). Roles:

- **admin** — everything: config editing, channel mapping, the bridge
  power switch, user management (create/delete, last admin cannot be
  deleted, you cannot delete yourself)
- **user** — read-only viewer of the dashboard (status, feed, charts)

Tabs: **Dashboard** (status cards, mapping editor, statistics, feed with
search/paging), **Config** (admin), **Users** (admin). The feed, filters
and charts are described above; the mapping editor stays on the
dashboard tab.

Runtime data files (all gitignored):

| File | Content |
|---|---|
| `stats.db` | SQLite: full message/event history, live configuration, users/sessions |
| `events.jsonl` | Rotating event log (last 150 records, legacy/compact) |
| `status.json` | Current state incl. config snapshot, updated every ~15 s |
| `channels.json` | Discovered channel names (after both sides connect) |

The dashboard's channel-credentials panel renders **live from the bridge
configuration** (admin only) — change the channel key on the Config page
and the panel updates within seconds.

> ⚠️ **Security**: the dashboard is reachable by anyone on your LAN
> (login protects the data, but don't expose it to untrusted networks).
> Config values include the MeshCore channel key — visible to admins
> only, but `stats.db`/`status.json` on the Pi are plain files: protect
> the host. The API blocks cross-origin POSTs (CSRF mitigation) and
> delays failed logins by 1 s.

## Delivery confirmation (MeshCore side)

When the bridge forwards a message **to** MeshCore (MT→MC direction), it
reports whether the message actually went out over the air:

1. **Repeat echo (exact)** — MeshCore group messages are encrypted with
   AES-128-ECB using the channel key, with no IV: the same plaintext
   always produces the same ciphertext. The bridge knows the channel key
   and the exact plaintext (timestamp + node name + text), so it computes
   the ciphertext that will be on the air. The companion radio pushes
   every raw received packet to the app (`PACKET_LOG_DATA` sniffer
   frames); when any node re-transmits our message, the payload matches
   **byte-for-byte** → event `mc_repeat` with the echo's SNR/RSSI and
   delay. No foreign traffic is ever decrypted.
2. **TX counter (best effort)** — if no echo arrives within
   `mc_confirm_window` (default 30 s), packet statistics
   (`CMD_GET_STATS`, firmware v8+) are compared before/after the send:
   a growing `sent` counter means the radio transmitted (an occasional
   advert can inflate it — treat as "air activity", not proof).
   Event `mc_confirm` with `mc_tx: true/false/null`.

Degradation: without `pycryptodome` (or without knowing the channel key)
echo matching is skipped; on old firmware without stats the event reports
`null`. An echo also requires our node to *hear* the repeater — no
repeater in range means "tx-only", which is not a failure.

Events appear on the dashboard with a green **repeater echo** badge
(filter *Forwarded*).

## Watchdog & health self-check

`mesh-bridge.service` runs as `Type=notify` with `WatchdogSec=180`: the
bridge sends `READY=1` on start and `WATCHDOG=1` at most every 60 s
(implemented via raw `sd_notify` datagrams — no python dependency).
If the event loop hangs, pings stop and systemd kills & restarts the
service.

The pings double as a **health self-check**: maintenance must complete
within `maintenance_interval_hours + health_grace_minutes`. If it
doesn't (e.g. some BLE call hangs in a way timeouts missed), the bridge
logs `HEALTH: ...`, writes a `health` event (visible on the dashboard)
and deliberately stops pinging — systemd restarts the whole process,
which so far has fixed every stuck-BLE state.

`status.json` carries `health_ok` and `last_maintenance` for other
monitoring tools.

## MQTT

Optional side for home automation / web clients (`[mqtt]` in the config,
editable on the Config page; changes reconnect the client within ~15 s).
Publishes (JSON, QoS 0):

| Topic | Content |
|---|---|
| `<prefix>/mc/recv` | `{"ts","channel","text"}` — every MeshCore channel message (all channels, not just bridged) |
| `<prefix>/mt/recv` | `{"ts","channel","src","text"}` — every Meshtastic channel message |
| `<prefix>/status` | Bridge status (retained, every 60 s): connections, health, counters, queues, mappings |
| `homeassistant/<type>/<uid>/config` | HA MQTT discovery (retained) — bridge entities appear in HA automatically (device *Mesh Bridge*: MeshCore/Meshtastic connectivity, health, forwarded counters, repeater echoes, queues, last messages) |

Inbound (`allow_inbound = true`, subscribe only):

| Topic | Payload |
|---|---|
| `<prefix>/send` | `{"side": "mc"\|"mt", "channel": <idx>, "text": "..."}` → sent into the mesh on that side (rate limits apply) |

> ⚠️ **Inbound security**: anyone with broker credentials can post into
> your mesh. Point the bridge at a trusted broker, use a dedicated
> login, and keep `allow_inbound` off unless you need it.

This is a readable JSON API (not the LetsMesh observer raw-packet
format); anything that can speak MQTT — Home Assistant, Node-RED,
scripts — can consume it.

## Daily maintenance

Once a day (configurable) the bridge:

1. **Syncs clocks** on both radios (messages and pruning depend on correct
   timestamps).
2. **Prunes MeshCore contacts** not modified for `mc_contact_max_age_days`
   (14 d default) via `CMD_REMOVE_CONTACT` — favorites are never touched.
3. **Prunes the Meshtastic nodeDB** — nodes not heard from for
   `mt_node_max_age_days` (7 d default) via `removeNode` — favorites and the
   local node are never touched.
4. At most `max_removals_per_run` deletions per run; `dry_run = true` only
   logs what would be deleted.

Messages cannot pile up on the radios: MeshCore keeps a RAM queue only
(16 messages, oldest overwritten) and vanilla Meshtastic firmware does not
persist text messages at all.

## Troubleshooting & gotchas

Collected the hard way — read this before filing issues:

- **meshtastic lib**: admin `writeConfig` without `node.ensureSessionKey()`
  is silently dropped by firmware (≥ 2.5). Always call it first.
- **`power.ls_secs = 0` does not mean "off"** — it means "default 300 s".
  BLE does not work in light sleep; set a large value (e.g. 86400).
  `setup_tbeam.py` does this for you.
- **Silent GATT death**: when the BLE connection to the Meshtastic node dies
  quietly, `meshtastic.connection.lost` never fires and `sendText`/`setTime`
  block *forever*. The bridge wraps every blocking call in a timeout
  (`MT_OP_TIMEOUT` 30 s, `MT_CONNECT_TIMEOUT` 120 s in `bridge.py`), marks
  the link broken and queues the message. Symptom without this hardening:
  status says `MT=OK` but counters freeze and maintenance never finishes.
- **Concurrent BLE scans break each other** (MeshCore client + meshtastic
  library on one adapter): the bridge always connects MT first, MC second;
  `create_channel.py` works the same way.
- **Stuck GATT connection after a failed handshake**: the device stops
  advertising; cleaned via `bluetoothctl disconnect <MAC>`.
- **MeshCore `CMD_SEND_CHANNEL_TXT_MSG` answers PACKET_OK (0x00)**, not
  MSG_SENT (0x06).
- **MeshCore firmware refuses setting the clock backwards**; the bridge
  sets time only when the drift exceeds 30 s.
- **Channel names in MeshCore must be lowercase** (the app rejects
  uppercase).
- **Watchdog needs `Type=notify`** — `WatchdogSec` only works for notify
  services; the unit template and the `sd_notify` implementation in
  `bridge.py` take care of this. Without systemd the bridge logs
  normally, the watchdog ping is a no-op.
- **Delivery confirmation silently degrades**: no `pycryptodome` → no
  echo matching; old MeshCore firmware (no `CMD_GET_STATS`) → no TX
  counter; the `PACKET_LOG_DATA` sniffer requires companion firmware
  that implements `logRxRaw` (recent versions do).

Useful commands:

```bash
journalctl -u mesh-bridge -f            # live log
journalctl -u mesh-bridge --since today # today's log
sudo systemctl restart mesh-bridge      # restart (picks up config.ini)
bluetoothctl info <MAC>                 # pairing state check
```

## Project layout

| File | Purpose |
|---|---|
| `bridge.py` | Main bridge: Meshtastic side, forwarding, queues, maintenance |
| `meshcore_ble.py` | MeshCore companion protocol over BLE (bleak) |
| `config_schema.py` | Shared config schema (bridge + dashboard) |
| `dashboard.py` | Web UI: login, feed/search/paging, charts, Config, users |
| `config.ini.example` | Configuration template — copy to `config.ini` |
| `pair_bt.py` | Non-interactive BlueZ pairing with fixed PINs |
| `create_channel.py` | One-off shared-channel setup on both devices |
| `setup_tbeam.py` | One-off Meshtastic radio setup (light sleep off) |
| `test_meshcore.py` | MeshCore BLE client smoke test |
| `mt_serial_listen.py` | Meshtastic serial packet listener (debug helper) |
| `install.sh` | Installation on the Pi (venv + systemd units) |
| `deploy.sh` | Maintainer helper: scp to the Pi (see `deploy.local`) |
| `mesh-bridge.service`, `mesh-dashboard.service` | systemd unit templates |
| `sudoers.d-mesh-bridge` | Optional sudoers rule for the power switch |

## Similar projects

- [Akita-Meshtastic-Meshcore-Bridge](https://github.com/AkitaEngineering/Akita-Meshtastic-Meshcore-Bridge) — Python bridge over **serial/MQTT** with TUI, REST API and rate limiting
- [hoplink](https://github.com/A13xB0/hoplink) — Go bridge over **TCP** (MeshCore ↔ Meshtastic ↔ Discord) with delivery confirmation via repeater tracking
- [MeshNard MT-MC relay](https://meshnard.com/mesh/mt-mc_relay) — minimal serial relay script
- MeshCore upstream deliberately does not plan Meshtastic interoperability
  ([meshcore-dev/MeshCore#1404](https://github.com/meshcore-dev/MeshCore/issues/1404)),
  so third-party bridges are the way.

This project differs by being **BLE-only on both ends** (no cables, no WiFi
on the radios), with a web dashboard, offline queues and device maintenance.

## Roadmap / TODO

Ideas for future versions — some inspired by the projects above
(the first seven have since been implemented, see *Delivery confirmation*,
*Watchdog hardening*, the i18n dashboard, *Persistent statistics*, the
auth + Config page, *MQTT* and *Rate limiting* above):

- [x] **Delivery confirmation** — track MeshCore repeater echoes to
      report whether a forwarded message actually went out over the air
      (à la hoplink; see above for how)
- [x] **Watchdog hardening** — systemd `WatchdogSec` + health self-check
      (maintenance overdue ⇒ restart)
- [x] **i18n** — English/CS dashboard with a flag toggle (default EN,
      cookie-stored)
- [x] **Persistent statistics** — SQLite-backed message/event history
      (searchable, paginated feed) and daily charts: forwarded messages,
      reconnects, uptime per side
- [x] **MQTT side** — bridge to/from an MQTT broker, feeding
      home-automation and web clients (readable JSON topics + HA
      discovery; see above)
- [x] **Rate limiting** — per-direction minimum interval between
      forwarded messages, applied to live traffic and queue flushes
- [ ] **REST API** with health/metrics/status endpoints and token auth
      (à la AMMB) for remote monitoring
- [x] **Dashboard authentication** — login/logout with PBKDF2-hashed
      users, 30-day sessions, admin (config/mapping/power/users) vs
      read-only viewers; config itself moved to SQLite with a Config page
- [ ] **Notifications** — alert on prolonged disconnects (e-mail / Telegram /
      gotify / MQTT)
- [ ] **DM bridging (opt-in)** — relay direct messages to a configured
      counterpart contact; off by default (privacy)
- [ ] **Position/node-info/telemetry forwarding** (opt-in) — e.g. MeshCore
      advert → Meshtastic position packet
- [ ] **Discord/Matrix gateway** (à la hoplink)
- [ ] **Multi-radio support** — more than one node per side
- [ ] **Packaging** — Docker image / PyPI package for non-Pi hosts

## License

GPL-3.0-or-later — see [LICENSE](LICENSE). Uses the
[meshtastic](https://github.com/meshtastic/python) python library
(Apache-2.0) and [bleak](https://github.com/hbldh/bleak) (MIT).
