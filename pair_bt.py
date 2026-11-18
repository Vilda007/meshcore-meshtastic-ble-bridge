#!/usr/bin/env python3
"""Neinteraktivni parovani BLE zarizeni s BlueZ (pevny PIN).

Pouziti (ve venv s bleak/dbus-fast, na Pi s bezicim bluetoothd):

    python3 pair_bt.py <MAC1> <PIN1> [<MAC2> <PIN2> ...]

Registruje BlueZ pairing agenta, ktery na zadost o PIN odpovi
nakonfigurovanym PINem pro dane zarizeni, spusti sken, zparuje a
nastavi Trusted=true. Zarizeni musí INZEROVAT (byt v dosahu a nezastinene,
T-Beam nesmi byt v light sleepu — pripadne ho probudit tlacitkem/reset).

Pozn.: pokud uz je zarizeni sparovane, jen se overi a nastavi Trusted.
"""

import asyncio
import sys

from dbus_fast import BusType, Message, MessageType, Variant
from dbus_fast.aio import MessageBus
from dbus_fast.service import ServiceInterface, method

AGENT_PATH = "/meshbridge/pairing"
BLUEZ = "org.bluez"
AGENT_IFACE = "org.bluez.Agent1"


class PinAgent(ServiceInterface):
    def __init__(self, pins):
        super().__init__(AGENT_IFACE)
        self.pins = {m.upper(): p for m, p in pins.items()}

    def _pin_for(self, path):
        mac = path.rsplit("dev_", 1)[-1].replace("_", ":").upper()
        return self.pins.get(mac, "123456")

    @method()
    def Release(self):
        print("[agent] Release")

    @method()
    def RequestPinCode(self, device: "o") -> "s":
        pin = self._pin_for(device)
        print(f"[agent] PIN pro {device}: {pin}")
        return pin

    @method()
    def DisplayPinCode(self, device: "o", pincode: "s"):
        print(f"[agent] zarizeni {device} zobrazuje PIN {pincode}")

    @method()
    def RequestConfirmation(self, device: "o", numeric: "u"):
        print(f"[agent] potvrzuji {device} (numeric {numeric})")

    @method()
    def AuthorizeService(self, device: "o", uuid: "s"):
        print(f"[agent] authorizuji {device} ({uuid})")

    @method()
    def Cancel(self):
        print("[agent] Cancel")


async def call(bus, msg, check=True):
    reply = await bus.call(msg)
    if check and reply.message_type == MessageType.ERROR:
        raise RuntimeError(f"D-Bus chyba: {reply.error_name}: "
                           f"{reply.body}")
    return reply


async def get_props(bus, path):
    reply = await call(bus, Message(
        destination=BLUEZ, path=path,
        interface="org.freedesktop.DBus.Properties",
        member="GetAll", signature="s", body=["org.bluez.Device1"]))
    return {k: v.value for k, v in reply.body[0].items()}


async def set_prop(bus, path, iface, prop, value):
    await call(bus, Message(
        destination=BLUEZ, path=path,
        interface="org.freedesktop.DBus.Properties",
        member="Set", signature="ssv",
        body=[iface, prop, Variant("b", value)]))


async def wait_for_device(bus, mac, adapter="hci0", timeout=120):
    dev_path = f"/org/bluez/{adapter}/dev_{mac.replace(':', '_')}"
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        reply = await call(bus, Message(
            destination=BLUEZ, path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="GetManagedObjects"))
        if dev_path in reply.body[0]:
            return dev_path
        await asyncio.sleep(2)
    raise TimeoutError(f"Zarizeni {mac} se v skenu neobjevilo "
                       f"({timeout} s). Zkontroluj napajeni/dosah/sleep.")


async def pair_one(bus, mac, pin):
    print(f"\n=== Paruji {mac} (PIN {pin}) ===")
    dev_path = await wait_for_device(bus, mac)
    props = await get_props(bus, dev_path)
    if props.get("Paired"):
        print(f"{mac}: uz sparovano, nastavuji Trusted")
        await set_prop(bus, dev_path, "org.bluez.Device1", "Trusted", True)
        return True
    reply = await call(bus, Message(
        destination=BLUEZ, path=dev_path,
        interface="org.bluez.Device1", member="Pair"), check=False)
    if reply.message_type == MessageType.ERROR:
        # AlreadyExists je OK
        if reply.error_name == "org.bluez.Error.AlreadyExists":
            print(f"{mac}: uz sparovano (AlreadyExists)")
        else:
            print(f"{mac}: parovani selhalo: {reply.error_name}: {reply.body}")
            return False
    else:
        print(f"{mac}: sparovano OK")
    await set_prop(bus, dev_path, "org.bluez.Device1", "Trusted", True)
    print(f"{mac}: Trusted=true")
    return True


async def main(pins):
    bus = await MessageBus(bus_type=BusType.SYSTEM).connect()

    agent = PinAgent(pins)
    bus.export(AGENT_PATH, agent)
    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez",
        interface="org.bluez.AgentManager1", member="RegisterAgent",
        signature="os",
        body=[AGENT_PATH, "Keyboard"]))
    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez",
        interface="org.bluez.AgentManager1", member="RequestDefaultAgent",
        signature="o", body=[AGENT_PATH]))
    print("Agent zaregistrovan jako default.")

    # adapter on + pairable
    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez/hci0",
        interface="org.freedesktop.DBus.Properties", member="Set",
        signature="ssv",
        body=["org.bluez.Adapter1", "Powered", Variant("b", True)]))
    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez/hci0",
        interface="org.freedesktop.DBus.Properties", member="Set",
        signature="ssv",
        body=["org.bluez.Adapter1", "Pairable", Variant("b", True)]))
    # sken
    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez/hci0",
        interface="org.bluez.Adapter1", member="StartDiscovery"))
    print("Sken zapnut...")

    results = []
    for mac, pin in pins.items():
        try:
            results.append(await pair_one(bus, mac, pin))
        except Exception as e:
            print(f"CHYBA u {mac}: {e}")
            results.append(False)

    await call(bus, Message(
        destination=BLUEZ, path="/org/bluez/hci0",
        interface="org.bluez.Adapter1", member="StopDiscovery"))
    await bus.send(Message(
        destination=BLUEZ, path="/org/bluez",
        interface="org.bluez.AgentManager1", member="UnregisterAgent",
        signature="o", body=[AGENT_PATH]))

    print("\n=== Vysledek ===")
    ok = True
    for (mac, _), r in zip(pins.items(), results):
        print(f"{mac}: {'OK' if r else 'SELHALO'}")
        ok = ok and r
    return 0 if ok else 1


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) < 2 or len(args) % 2 != 0:
        print(__doc__)
        sys.exit(2)
    pins = dict(zip(args[0::2], args[1::2]))
    sys.exit(asyncio.run(main(pins)))