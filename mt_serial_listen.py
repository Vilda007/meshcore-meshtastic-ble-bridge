import sys, time, datetime, os
import meshtastic
import meshtastic.serial_interface
from pubsub import pub

DURATION_S = int(sys.argv[1]) if len(sys.argv) > 1 else 600
SERIAL_PORT = sys.argv[2] if len(sys.argv) > 2 else "COM4"
BASE = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE, f"mt_serial_listen_{int(time.time())}.txt")

lines = []

def on_receive(packet, interface):
    decoded = packet.get("decoded", {})
    port = decoded.get("portnum", "?")
    text = decoded.get("text", "")
    from_id = packet.get("fromId", "?")
    to_id = packet.get("toId", "?")
    line = f"{datetime.datetime.now():%H:%M:%S} PKT from={from_id} to={to_id} port={port}"
    if text:
        line += f' text="{text}"'
    print(line, flush=True)
    lines.append(line)

pub.subscribe(on_receive, "meshtastic.receive")

iface = meshtastic.serial_interface.SerialInterface(devPath=SERIAL_PORT)
print(f"{SERIAL_PORT} listener running for {DURATION_S} s...", flush=True)

start = time.time()
try:
    while time.time() - start < DURATION_S:
        time.sleep(2)
finally:
    iface.close()
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"zapsano {len(lines)} paketu do {LOG_FILE}")