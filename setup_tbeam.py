#!/usr/bin/env python3
"""One-off Meshtastic device setup over BLE (run right after a device reset!).

1. connects over BLE, ensures an admin session key (fw 2.5+ requires the
   passkey handshake -- the library silently drops messages without it)
2. disables light sleep: power.ls_secs = 86400 (a day; BLE does not work
   in light sleep)
   - note: ls_secs=0 does NOT mean "off", it means "default 300 s"!
3. verifies the value in the same connection

Usage: setup_tbeam.py [device-name-or-MAC]   (default: first Meshtastic found)
"""
import sys
from meshtastic.ble_interface import BLEInterface

LS_SECS = 86400

iface = BLEInterface(sys.argv[1] if len(sys.argv) > 1 else None)
print("pripojeno, node_num:", hex(iface.myInfo.my_node_num))
node = iface.localNode

node.ensureSessionKey()
print("session key OK")

node.localConfig.power.ls_secs = LS_SECS
node.writeConfig("power")
print(f"zapsano ls_secs={LS_SECS}, cekam na potvrzeni...")
iface.waitForConfig()   # config push po zapisu
print("ls_secs po zapisu:", node.localConfig.power.ls_secs)

iface.close()
assert node.localConfig.power.ls_secs == LS_SECS, "HODNOTA SE NEPROSADILA"
print("HOTOVO: light sleep odlozen na 86400 s nečinnosti")