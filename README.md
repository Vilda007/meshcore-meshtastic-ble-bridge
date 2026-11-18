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
- **Web dashboard** (port 8420, zero dependencies): live status,
  message/event feed with filters, a power switch for the bridge service,
  and the channel-mapping editor.
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
  `bleak`, `meshtastic`, `dbus-fast`, `pubsub`)

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

`config.ini` — full reference (see `config.ini.example` for a commented
template):

| Section | Option | Default | Meaning |
|---|---|---|---|
| `[meshcore]` | `device` | *(first found)* | Substring of the MeshCore BLE name |
| | `channel` | `0` | Default MC channel (fallback, no `[channels]` mappings) |
| | `poll_interval` | `30` | Seconds between polls / keep-alives |
| `[meshtastic]` | `device` | *(first found)* | Meshtastic BLE name or MAC |
| | `channel` | `0` | Default MT channel (fallback) |
| `[channels]` | `mappings` | `0:0` | Bridged pairs `mc:mt, mc:mt, …`; runtime editable from the dashboard (`mappings.json` takes precedence) |
| | `mc_channel_name` | `#meshbridge` | MC name of the shared channel (lowercase only!) |
| | `mc_channel_key` | *(required)* | **16-byte hex** MeshCore channel key — a SECRET, never commit it |
| | `mt_channel_name` | `#meshbridge` | MT channel name |
| | `mt_channel_psk` | `AQ==` | MT PSK; `AQ==` = default public key (joinable by name) |
| `[bridge]` | `prefix_to_mt` / `prefix_to_mc` | `MC:` / `MT:` | Forwarding prefixes (loop guard) |
| | `mc_max_len` / `mt_max_len` | `120` / `200` | Max body length per side |
| | `dedup_cache` | `400` | LRU dedup cache entries |
| | `log_level` | `INFO` | Logging verbosity |
| | `log_file` | *(empty)* | Rotating log file; empty = journal only |
| | `event_log` / `status_file` | `events.jsonl` / `status.json` | Dashboard data files |
| `[maintenance]` | `mc_contact_max_age_days` | `14` | Prune MC contacts older than this |
| | `mt_node_max_age_days` | `7` | Prune MT nodeDB nodes older than this |
| | `maintenance_interval_hours` | `24` | Maintenance period |
| | `max_removals_per_run` | `10` | Removal safety limit |
| | `dry_run` | `false` | Log only, delete nothing |

**What you must replace before use** (all gitignored — never commit):

- `config.ini` — your device names, your **channel key**
- `channel_info.json` (optional, from `channel_info.json.example`) — shown on
  the dashboard panel; contains credentials
- `deploy.local` (optional) — your SSH alias for `deploy.sh`

## Shared channel (#meshbridge)

MeshCore has no "public channels by name" — a channel *is* its 16-byte key
(`SHA256(key)` = channel hash). The Meshtastic convention is the opposite:
a channel is public when it uses the default PSK (`AQ==`), joinable by name.

`create_channel.py` sets up the same-named channel on both devices
(idempotent, reads everything from `config.ini`):

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

- **Status cards** — MC/MT connection, forwarded message counters, queue
  depth, maintenance stats
- **Power switch** — starts/stops the `mesh-bridge` service via
  `sudo systemctl --no-block` (works with NOPASSWD sudo or the optional
  sudoers rule)
- **Channel mapping editor** — bijective MC↔MT pairs, validated both ways;
  the bridge picks changes up within ~15 s
- **Event feed** — receive / forward / filtered / queued / flushed / connect /
  disconnect / maintenance, with filters and 5 s auto-refresh; timestamps in
  `YYYY-MM-DD HH:MM:SS`
- **Channel credentials panel** — only if `channel_info.json` exists

Runtime data files (all gitignored):

| File | Content |
|---|---|
| `events.jsonl` | Rotating event log (last 150 records) |
| `status.json` | Current state, updated every ~15 s |
| `mappings.json` | Live channel mapping (dashboard writes, bridge reads) |
| `channels.json` | Discovered channel names (after both sides connect) |
| `channel_info.json` | Optional credentials for the dashboard panel |

> ⚠️ **Security**: the dashboard has **no authentication** and is reachable
> by anyone on your LAN — including the bridge power switch. Expose it to a
> trusted home network only (or add a reverse proxy with auth / firewall
> rules if that bothers you).

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
| `dashboard.py` | Web UI (stdlib http server, port 8420) |
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

Ideas for future versions — some inspired by the projects above:

- [ ] **MQTT side** — bridge to/from an MQTT broker (MeshCore observer
      format), so the bridge can feed home-automation and web clients
      (à la AMMB)
- [ ] **REST API** with health/metrics/status endpoints and token auth
      (à la AMMB) for remote monitoring
- [ ] **Delivery confirmation** — track MeshCore repeater/observer echoes to
      report whether a forwarded message actually went out over the air
      (à la hoplink)
- [ ] **Rate limiting** — configurable per-window caps to prevent channel
      flooding
- [ ] **Dashboard authentication** — optional token/basic-auth (+ HTTPS
      behind a reverse proxy) so the power switch is not LAN-public
- [ ] **Persistent statistics** — SQLite-backed counters and history charts
      (messages/day, uptime, reconnects)
- [ ] **Notifications** — alert on prolonged disconnects (e-mail / Telegram /
      gotify / MQTT)
- [ ] **DM bridging (opt-in)** — relay direct messages to a configured
      counterpart contact; off by default (privacy)
- [ ] **Position/node-info/telemetry forwarding** (opt-in) — e.g. MeshCore
      advert → Meshtastic position packet
- [ ] **Discord/Matrix gateway** (à la hoplink)
- [ ] **Multi-radio support** — more than one node per side
- [ ] **Packaging** — Docker image / PyPI package for non-Pi hosts
- [ ] **i18n** — English/CS dashboard (currently Czech)
- [ ] **Watchdog hardening** — systemd `WatchdogSec` + health self-check
      (e.g. fail if maintenance has not completed in X hours)

## License

GPL-3.0-or-later — see [LICENSE](LICENSE). Uses the
[meshtastic](https://github.com/meshtastic/python) python library
(Apache-2.0) and [bleak](https://github.com/hbldh/bleak) (MIT).
