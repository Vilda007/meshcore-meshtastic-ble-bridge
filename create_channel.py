#!/usr/bin/env python3
"""One-off setup of a shared channel on both devices (creates if missing).

  MeshCore:   a channel is defined by its KEY, not its name
              (SHA256(key) = the channel hash). The script finds the channel
              by name and verifies its key against `mc_channel_key` from
              config.ini -- or creates it on a free slot. Channel names must
              be lowercase (the MeshCore app rejects uppercase letters).
  Meshtastic: first free slot -> SECONDARY channel with the configured name
              and PSK (default "AQ==", the public-by-convention key like the
              default Meshtastic channels -- anyone can join by adding a
              channel with the same name and default key).

Channel name and keys are read from config.ini ([channels] section) --
NEVER hardcode channel keys in scripts you intend to share.

The MT connection is made FIRST, MC only afterwards: a MeshCore BLE scan
breaks the meshtastic library handshake in the same process (the same
reason the bridge uses a staggered start). The bridge service must be
stopped while this runs. The script is idempotent.

Usage: create_channel.py [phase] [config.ini]
       phase = mc | mt | all   (each phase ideally in its own process)
"""

import asyncio
import base64
import configparser
import json
import logging
import os
import subprocess
import sys
import time

from bridge import Config
from meshcore_ble import MeshCoreClient

from meshtastic.ble_interface import BLEInterface
from meshtastic.protobuf import channel_pb2


def read_channel_cfg(cfg_path):
    """(mc_name, mc_key, mt_name, mt_psk) from [channels] in config.ini."""
    ini = configparser.ConfigParser()
    if os.path.exists(cfg_path):
        ini.read(cfg_path, encoding="utf-8")
    ch = ini["channels"] if ini.has_section("channels") else {}

    mc_name = ch.get("mc_channel_name", "#meshbridge").strip()
    key_hex = ch.get("mc_channel_key", "").strip().replace(" ", "")
    if not key_hex:
        raise SystemExit(
            "config.ini [channels]: mc_channel_key is required "
            "(16-byte hex string -- the MeshCore channel key; generate one, "
            "e.g. `python3 -c \"import os;print(os.urandom(16).hex())\"`)")
    try:
        mc_key = bytes.fromhex(key_hex)
    except ValueError:
        raise SystemExit("config.ini [channels]: mc_channel_key is not hex")
    if len(mc_key) != 16:
        raise SystemExit("config.ini [channels]: mc_channel_key must be "
                          "16 bytes (32 hex chars)")

    mt_name = ch.get("mt_channel_name", mc_name).strip()
    psk_b64 = ch.get("mt_channel_psk", "AQ==").strip()
    try:
        mt_psk = base64.b64decode(psk_b64)
    except Exception:
        raise SystemExit("config.ini [channels]: mt_channel_psk must be "
                          "base64 (default \"AQ==\" = public key)")
    return mc_name, mc_key, mt_name, mt_psk


def _bluez_disconnect(needle):
    """Drop a stuck GATT connection (same as bridge._bluez_cleanup_mt)."""
    try:
        out = subprocess.run(["bluetoothctl", "devices"],
                             capture_output=True, text=True,
                             timeout=15).stdout
        for line in out.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            if needle.lower() in line.lower():
                subprocess.run(["bluetoothctl", "disconnect", parts[1]],
                               capture_output=True, timeout=15)
                print(f"MT: cleaned up stuck connection {parts[1]}")
    except Exception as e:
        print(f"MT: bluez cleanup failed: {e!r}")


def create_mt(cfg, mt_name, mt_psk):
    """Return (channel index, channel list); create the channel if needed.

    The first handshake with the device often fails and leaves a stuck GATT
    connection (as in the bridge) -- hence retries with cleanup + backoff.
    """
    iface = None
    last_err = None
    for attempt in range(5):
        try:
            iface = BLEInterface(cfg.mt_device or None)
            break
        except Exception as e:
            last_err = e
            print(f"MT: connect failed ({e!r}), attempt {attempt + 1}/5")
            _bluez_disconnect(cfg.mt_device or "meshtastic")
            time.sleep(5 + 5 * attempt)
    if iface is None:
        raise SystemExit(f"MT: could not connect: {last_err!r}")
    try:
        node = iface.localNode
        ch = node.getChannelByName(mt_name)
        if ch is None:
            ch = node.getDisabledChannel()
            if not ch:
                raise SystemExit("MT: no free channel slot (of 8)")
            chs = channel_pb2.ChannelSettings()
            chs.psk = mt_psk
            chs.name = mt_name
            ch.settings.CopyFrom(chs)
            ch.role = channel_pb2.Channel.Role.SECONDARY
            node.writeChannel(ch.index)  # itself calls ensureSessionKey()
            time.sleep(3)  # let the firmware persist the config
            ver = node.getChannelByChannelIndex(ch.index)
            if not (ver and ver.settings.name == mt_name):
                raise SystemExit(f"MT: verification failed: {ver!r}")
            print(f"MT: channel {mt_name!r} created at index {ch.index} "
                  f"(role SECONDARY, psk configured)")
        else:
            print(f"MT: channel {mt_name!r} already exists at index {ch.index}")
        out = []
        for i, c in enumerate(node.channels or []):
            name = (getattr(c.settings, "name", "")
                    if c.HasField("settings") else "") or ""
            if int(getattr(c, "role", 1)) == 0 and not name:  # DISABLED
                continue
            out.append({"idx": i, "name": name})
        print("MT channels:", ", ".join(
            f"{c['idx']}:{c['name']!r}" for c in out))
        return ch.index, out
    finally:
        iface.close()


async def create_mc(cfg, mc_name, mc_key):
    """Return (channel index, channel list); create the channel if needed.

    Hard budget of 7 minutes: a BLE scan in a process that has used the
    meshtastic library can hang, hence the hard process kill.
    """
    try:
        return await asyncio.wait_for(_create_mc(cfg, mc_name, mc_key), 420)
    except asyncio.TimeoutError:
        print("MC: 7 min budget exceeded -- killing process (kernel cleans BLE)")
        os._exit(1)


async def _create_mc(cfg, mc_name, mc_key):
    async def nop(*args):
        pass

    mc = MeshCoreClient(nop, name_filter=cfg.mc_device,
                        poll_interval=30.0, app_name="mesh-setup")
    task = asyncio.create_task(mc.run())
    try:
        for _ in range(100):  # a BLE scan can take minutes
            if mc.connected:
                break
            await asyncio.sleep(3)
        else:
            raise SystemExit("MC: could not connect (5 min scan)")

        max_ch = (mc.device_info or {}).get("max_channels") or 8
        slots = {}
        for i in range(max_ch):
            ch = await mc.get_channel(i)
            if ch:
                slots[i] = ch
        print("MC channels:", ", ".join(
            f"{i}:{ch['name']!r}" for i, ch in sorted(slots.items())))

        existing = [i for i, ch in slots.items() if ch["name"] == mc_name]
        if existing:
            idx = existing[0]
            if slots[idx]["secret"] != mc_key.hex():
                raise SystemExit(f"MC: channel {mc_name!r} has a different "
                                 "key than mc_channel_key in config.ini!")
            print(f"MC: channel {mc_name!r} exists at index {idx}")
        else:
            free = next((i for i in range(max_ch) if i not in slots), None)
            if free is None:
                raise SystemExit(f"MC: no free slot ({max_ch})")
            if not await mc.set_channel(free, mc_name, mc_key):
                raise SystemExit(f"MC: set_channel({free}) rejected")
            idx = free
            print(f"MC: channel {mc_name!r} created at index {idx}")

        ver = await mc.get_channel(idx)
        if not (ver and ver["name"] == mc_name
                and ver["secret"] == mc_key.hex()):
            raise SystemExit(f"MC: verification failed: {ver!r}")
        print(f"MC: verified -- index {idx}, name {ver['name']!r}, "
              f"key (base64) {base64.b64encode(mc_key).decode()}")
        out = [{"idx": i, "name": c["name"]}
               for i, c in sorted(slots.items()) if c["name"]]
        return idx, out
    finally:
        await mc.stop()
        task.cancel()


def main():
    # phase "mc" / "mt" / "all" -- ideally each phase in its own process
    # (a meshtastic BLEInterface session breaks a subsequent MeshCore
    #  bleak scan in the same process)
    args = sys.argv[1:]
    if args and args[0] in ("mc", "mt", "all"):
        phase = args.pop(0)
    else:
        phase = "all"
    cfg_path = args[0] if args else "config.ini"
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Config(cfg_path)
    mc_name, mc_key, mt_name, mt_psk = read_channel_cfg(cfg_path)
    print("NOTE: the mesh-bridge service must be stopped "
          "(sudo systemctl stop mesh-bridge).")

    # channels.json: load and update only the side we processed
    data = {"updated": int(time.time()), "mc": [], "mt": []}
    try:
        with open("channels.json", encoding="utf-8") as f:
            data.update(json.load(f))
    except (OSError, json.JSONDecodeError):
        pass
    data["updated"] = int(time.time())

    if phase in ("all", "mt"):
        mt_idx, data["mt"] = create_mt(cfg, mt_name, mt_psk)
        print(f"MT index: {mt_idx}")
    else:
        mt_idx = None
    if phase in ("all", "mc"):
        mc_idx, data["mc"] = asyncio.run(create_mc(cfg, mc_name, mc_key))
        print(f"MC index: {mc_idx}")
    else:
        mc_idx = None

    try:
        with open("channels.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        print("channels.json written (channel names for the dashboard)")
    except OSError as e:
        print(f"could not write channels.json: {e!r}")
    print()
    if mc_idx is not None and mt_idx is not None:
        print(f"DONE: dashboard mapping (mappings.json): [[{mc_idx}, {mt_idx}]]")
    else:
        print(f"Done for phase {phase!r}; indices: MC={mc_idx} MT={mt_idx}")


if __name__ == "__main__":
    main()
