#!/usr/bin/env python3
"""Cte / nastavuje konfiguraci auto-add kontaktu MeshCore radia.

Pouziti (na Pi, idealne se zastavenym mesh-bridge -- radio drzi jen
jedno BLE spojeni):

    python3 set_mc_autoadd.py [config.ini]              # pouze vypis aktualniho stavu
    python3 set_mc_autoadd.py [config.ini] --overwrite  # zapne prepsani nejstarsiho
                                                       # nefavoritniho kontaktu
                                                       # pri plne tabulce (bit 0)

Co se nastavuje (MyMesh.cpp CMD_SET_AUTOADD_CONFIG=58 / GET=59,
RESP_CODE_AUTOADD_CONFIG=25):

  flags bit 0 (0x01) = pri plne tabulce kontaktu (MAX_CONTACTS=100)
                       prepsat NEJSTARSI NEFAVORITNI kontakt
                       ( klient dostane CONTACT_DELETED 0x8F;
                         bez bitu 0 fw nove kontakty odmita )
  flags bity 1-4      = ktere typy se auto-adduji
                       (0x02 chat, 0x04 repeater, 0x08 room server)
  max_hops           = jen kontakty v dosahu N hopu (--overwrite hodnotu
                       zachova; bez udani fw necha stavajici)

Pozn.: Meshtastic strana ekvivalent NEPOTREBUJE -- fw pri plne nodeDB
samy prepsuje nejstarsi ne-favoritni uzel (NodeDB::getOrCreateMeshNode).
"""

import asyncio
import logging
import sys

from bridge import Config
from meshcore_ble import MeshCoreClient

AUTO_ADD_OVERWRITE_OLDEST = 0x01

BITS = [(0x01, "prepsat nejstarsi nefavoritni pri plne tabulce"),
        (0x02, "auto-add: chat (companion)"),
        (0x04, "auto-add: repeater"),
        (0x08, "auto-add: room server")]


def describe(flags):
    on = [desc for bit, desc in BITS if flags & bit]
    return "; ".join(on) if on else "(nic -- vse vypnuto)"


async def run(cfg, set_overwrite):
    async def nop(*args):
        pass

    mc = MeshCoreClient(nop, name_filter=cfg.mc_device,
                        poll_interval=30.0, app_name="mesh-setup")
    task = asyncio.create_task(mc.run())
    try:
        for _ in range(100):        # BLE scan muze trvat minuty
            if mc.connected:
                break
            await asyncio.sleep(3)
        else:
            raise SystemExit("MC: nepodarilo se pripojit (5 min scan)")

        cur = await mc.get_autoadd_config()
        if cur is None:
            raise SystemExit("MC: CMD_GET_AUTOADD_CONFIG zamitnuto "
                             "(starsi firmware bez auto-add configu?)")
        print(f"autoadd_config: flags=0x{cur['flags']:02x}, "
              f"max_hops={cur['max_hops']}")
        print(f"  bity: {describe(cur['flags'])}")

        if not set_overwrite:
            return

        new_flags = cur["flags"] | AUTO_ADD_OVERWRITE_OLDEST
        if new_flags == cur["flags"]:
            print("overwrite-oldest uz je zapnuty -- nic se nemeni")
            return
        if not await mc.set_autoadd_config(new_flags, cur["max_hops"]):
            raise SystemExit("MC: CMD_SET_AUTOADD_CONFIG zamitnuto")
        ver = await mc.get_autoadd_config()
        if ver is None or ver["flags"] != new_flags:
            raise SystemExit(f"MC: verifikace selhala (cte zpet {ver!r})")
        print(f"OK: flags=0x{ver['flags']:02x} "
              f"(max_hops={ver['max_hops']}), "
              f"bity: {describe(ver['flags'])}")
    finally:
        await mc.stop()
        task.cancel()


def main():
    args = [a for a in sys.argv[1:]]
    set_overwrite = "--overwrite" in args
    args = [a for a in args if a != "--overwrite"]
    cfg_path = args[0] if args else "config.ini"
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    cfg = Config(cfg_path)
    asyncio.run(run(cfg, set_overwrite))


if __name__ == "__main__":
    main()
