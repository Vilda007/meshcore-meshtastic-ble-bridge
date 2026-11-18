#!/bin/bash
# Install the bridge on a Raspberry Pi (or any Debian Linux with Bluetooth).
# Run ON THE PI, in the directory containing this file.
# Assumes: Raspberry Pi OS (Debian), onboard Bluetooth enabled.
set -e

BRIDGE_DIR="$(cd "$(dirname "$0")" && pwd)"
echo "Installing into: $BRIDGE_DIR"

if [ ! -f config.ini ]; then
    if [ -f config.ini.example ]; then
        echo "NOTE: config.ini not found -- creating it from the example."
        echo "      Edit it (device names, channel key!) before starting."
        cp config.ini.example config.ini
    else
        echo "ERROR: config.ini (or config.ini.example) not found."
        exit 1
    fi
fi

# 1) system dependencies
sudo apt-get update
sudo apt-get install -y python3-venv

# 2) venv + libraries
if [ ! -d "$BRIDGE_DIR/venv" ]; then
    python3 -m venv "$BRIDGE_DIR/venv"
fi
"$BRIDGE_DIR/venv/bin/pip" install --upgrade pip
# pycryptodome je volitelne (repeat-echo potvrzeni doruceni); bez nej
# bridge bezi jen s TX-counter potvrzenim
"$BRIDGE_DIR/venv/bin/pip" install bleak meshtastic pycryptodome
"$BRIDGE_DIR/venv/bin/python" -c "import bleak, meshtastic; print('libraries OK')"

# 3) clock (for reliable timestamps)
timedatectl show -p NTPSynchronized --value | grep -qiE "1|yes" || \
    echo "WARNING: the clock is not NTP synchronized."

# 4) systemd units (bridge + dashboard) -- fill in user/home placeholders
BRIDGE_USER="${SUDO_USER:-$USER}"
BRIDGE_HOME="$(getent passwd "$BRIDGE_USER" | cut -d: -f6)"
echo "Systemd services will run as: $BRIDGE_USER ($BRIDGE_HOME)"
for unit in mesh-bridge.service mesh-dashboard.service; do
    sed -e "s|__BRIDGE_USER__|$BRIDGE_USER|g" \
        -e "s|__BRIDGE_HOME__|$BRIDGE_HOME|g" \
        "$BRIDGE_DIR/$unit" | sudo tee "/etc/systemd/system/$unit" > /dev/null
done
sudo systemctl daemon-reload
sudo systemctl enable mesh-bridge mesh-dashboard

# 5) sudoers (OPTIONAL): only needed if your user has no NOPASSWD sudo
#    (otherwise the dashboard power switch cannot run systemctl):
#      sed "s|__BRIDGE_USER__|$BRIDGE_USER|g" sudoers.d-mesh-bridge \
#        | sudo install -m 0440 /dev/stdin /etc/sudoers.d/mesh-bridge
#      sudo visudo -cf /etc/sudoers.d/mesh-bridge

echo
echo "Done. Now:"
echo "  1. edit config.ini (device names, channel key)"
echo "  2. pair the devices:    venv/bin/python pair_bt.py <MAC_MC> <PIN_MC> <MAC_MT> <PIN_MT>"
echo "  3. test run:            venv/bin/python bridge.py   (Ctrl+C to quit)"
echo "  4. start the services:  sudo systemctl start mesh-bridge mesh-dashboard"
echo "  5. dashboard:            http://<pi-ip>:8420/   (find the IP: ip -4 addr)"
