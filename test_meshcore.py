#!/usr/bin/env python3
"""Quick test of the MeshCore BLE client: connect, clock, SELF_INFO, channels.

Usage: test_meshcore.py [device-name]   (default: first MeshCore found)
"""
import asyncio
import logging
import sys

from meshcore_ble import MeshCoreClient

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


async def main():
    name = sys.argv[1] if len(sys.argv) > 1 else None
    got = asyncio.Event()
    info_holder = {}

    async def on_msg(text, chan, ts):
        info_holder["msg"] = (text, chan, ts)
        print(f"ZPRAVA kanal={chan} ts={ts}: {text!r}")
        got.set()

    mc = MeshCoreClient(on_msg, name_filter=name, poll_interval=5.0)
    task = asyncio.create_task(mc.run())
    await asyncio.sleep(15)  # pockat na pripojeni
    print(f"connected={mc.connected} info={mc.info.get('name')} "
          f"device_info={mc.device_info}")
    if not mc.connected:
        print("NEPODARILO SE PRIPOJIT")
        task.cancel()
        return
    print("Cekam 60 s na zpravy (posli neco z jineho uzlu)...")
    try:
        await asyncio.wait_for(got.wait(), timeout=60)
    except asyncio.TimeoutError:
        print("Zadna zprava nedorazila (ok, pokud sit ticha).")
    ok = await mc.send_channel_message(0, "Bridge test: ok?")
    print(f"send test zpravy: {ok}")
    await asyncio.sleep(2)
    contacts = await mc.get_contacts()
    if contacts is None:
        print("GET_CONTACTS selhalo")
    else:
        print(f"kontaktu: {len(contacts)}")
        for c in contacts[:10]:
            age = (import_time() - c["lastmod"]) / 86400
            print(f"  {c['name'][:20]:20s} fav={c['is_favorite']} "
                  f"posledni zmena {age:.0f} d")
    await asyncio.sleep(30)  # pozorujeme stabilitu spojeni
    print(f"konec: connected={mc.connected}")
    await mc.stop()
    task.cancel()


def import_time():
    import time as t
    return t.time()


asyncio.run(main())