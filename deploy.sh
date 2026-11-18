#!/bin/bash
# Deploy from your workstation to the Pi running the bridge.
# Target host: PI env var, or a deploy.local file (gitignored), e.g.:
#   PI=my-pi            # your ssh alias from ~/.ssh/config
# Files that do not exist locally are skipped.
set -e

# maintainer's local override (never committed)
[ -f deploy.local ] && . deploy.local
PI="${PI:-raspberrypi.local}"
DEST="${DEST:-/home/pi/mesh-bridge}"   # adjust to your user on the Pi

FILES="bridge.py meshcore_ble.py config_schema.py pair_bt.py dashboard.py
       create_channel.py config.ini config.ini.example install.sh
       mesh-bridge.service mesh-dashboard.service sudoers.d-mesh-bridge
       README.md LICENSE .gitignore
       setup_tbeam.py test_meshcore.py mt_serial_listen.py"

# optional local-only files (exist only on the maintainer's machine)
OPTIONAL="CLAUDE.md deploy.sh"

echo "== copying files to $PI:$DEST =="
ssh "$PI" "mkdir -p $DEST"
for f in $FILES $OPTIONAL; do
    [ -f "$f" ] && scp -q "$f" "$PI:$DEST/" && echo "  ok: $f"
done

echo
echo "Done. On the Pi:"
echo "  ssh $PI"
echo "  cd $DEST && bash install.sh   # only on first install / dependency changes"
echo "  sudo systemctl restart mesh-bridge mesh-dashboard"
echo "  journalctl -u mesh-bridge -f       # logs"
echo "  dashboard: http://<pi-ip>:8420/"
