#!/usr/bin/env python3
"""Schema runtime konfigurace bridge -- sdili ho bridge.py a dashboard.py.

Konfigurace ma tri vrstvy (nizsi prepisuje vyssi):

  1. defaulty v kodu (bridge.Config)
  2. config.ini (bootstrap; stejne klice jako schema, sekce.tecka.klic)
  3. SQLite tabulka 'config' ve stats.db (ridi ji dashboard Config stranka;
     klice presne dle tohoto schematu + specialni klice 'config.updated')

Bridge pri startu nacte ini a prekryje ho hodnotami z DB; za behu hlida
'config.updated' (flush smycka, do 15 s) a zmeny projevi -- bud hned
(restart=False), nebo po restartu sluzby (restart=True; zaloguje se).
"""

CONFIG_SCHEMA = {
    # ---------------- MeshCore ----------------
    "meshcore.device": {
        "label": "MeshCore device (BLE name substring)",
        "type": "str", "restart": True, "section": "MeshCore"},
    "meshcore.channel": {
        "label": "Fallback MC channel (without [channels] mappings)",
        "type": "int", "min": 0, "max": 63, "restart": False,
        "section": "MeshCore"},
    "meshcore.poll_interval": {
        "label": "MC poll / keep-alive interval (s)",
        "type": "float", "min": 5, "max": 600, "restart": False,
        "section": "MeshCore"},
    # ---------------- Meshtastic ----------------
    "meshtastic.device": {
        "label": "Meshtastic device (BLE name or MAC)",
        "type": "str", "restart": True, "section": "Meshtastic"},
    "meshtastic.channel": {
        "label": "Fallback MT channel (without [channels] mappings)",
        "type": "int", "min": 0, "max": 7, "restart": False,
        "section": "Meshtastic"},
    # ---------------- bridge ----------------
    "bridge.prefix_to_mt": {
        "label": "Prefix added when forwarding MC -> MT",
        "type": "str", "restart": False, "section": "Bridge"},
    "bridge.prefix_to_mc": {
        "label": "Prefix added when forwarding MT -> MC",
        "type": "str", "restart": False, "section": "Bridge"},
    "bridge.mc_max_len": {
        "label": "Max body length towards MeshCore (~133 limit)",
        "type": "int", "min": 20, "max": 200, "restart": False,
        "section": "Bridge"},
    "bridge.mt_max_len": {
        "label": "Max body length towards Meshtastic (~233 limit)",
        "type": "int", "min": 20, "max": 230, "restart": False,
        "section": "Bridge"},
    "bridge.dedup_cache": {
        "label": "Dedup LRU cache size (messages)",
        "type": "int", "min": 50, "max": 5000, "restart": False,
        "section": "Bridge"},
    "bridge.log_level": {
        "label": "Log level",
        "type": "enum", "choices": ["DEBUG", "INFO", "WARNING", "ERROR"],
        "restart": False, "section": "Bridge"},
    "bridge.mc_confirm_window": {
        "label": "Repeat-echo confirmation window (s; 0 = off)",
        "type": "int", "min": 0, "max": 600, "restart": False,
        "section": "Bridge"},
    # ---------------- maintenance ----------------
    "maintenance.mc_contact_max_age_days": {
        "label": "Prune MC contacts older than (days)",
        "type": "int", "min": 1, "max": 365, "restart": False,
        "section": "Maintenance"},
    "maintenance.mt_node_max_age_days": {
        "label": "Prune MT nodeDB nodes older than (days)",
        "type": "int", "min": 1, "max": 365, "restart": False,
        "section": "Maintenance"},
    "maintenance.maintenance_interval_hours": {
        "label": "Maintenance period (hours)",
        "type": "float", "min": 1, "max": 720, "restart": False,
        "section": "Maintenance"},
    "maintenance.max_removals_per_run": {
        "label": "Max removals per maintenance run",
        "type": "int", "min": 0, "max": 100, "restart": False,
        "section": "Maintenance"},
    "maintenance.health_grace_minutes": {
        "label": "Health-check grace over the period (minutes)",
        "type": "int", "min": 5, "max": 1440, "restart": False,
        "section": "Maintenance"},
    "maintenance.dry_run": {
        "label": "Dry run (only log what would be removed)",
        "type": "bool", "restart": False, "section": "Maintenance"},
    # ---------------- shared channel (create_channel.py) ----------------
    "channels.mc_channel_name": {
        "label": "MeshCore channel name (lowercase only)",
        "type": "str", "restart": False, "section": "Shared channel"},
    "channels.mc_channel_key": {
        "label": "MeshCore channel key (16-byte hex) -- SECRET",
        "type": "hex16", "restart": False, "secret": True,
        "section": "Shared channel"},
    "channels.mt_channel_name": {
        "label": "Meshtastic channel name",
        "type": "str", "restart": False, "section": "Shared channel"},
    "channels.mt_channel_psk": {
        "label": "Meshtastic PSK (base64; AQ== = public default)",
        "type": "b64", "restart": False, "section": "Shared channel"},
}


# klic -> atribut tridy bridge.Config (aplikace overlay/zmen za behu)
CONFIG_ATTR = {
    "meshcore.device": "mc_device",
    "meshcore.channel": "mc_channel",
    "meshcore.poll_interval": "mc_poll_interval",
    "meshtastic.device": "mt_device",
    "meshtastic.channel": "mt_channel",
    "bridge.prefix_to_mt": "prefix_to_mt",
    "bridge.prefix_to_mc": "prefix_to_mc",
    "bridge.mc_max_len": "mc_max_len",
    "bridge.mt_max_len": "mt_max_len",
    "bridge.dedup_cache": "dedup_cache",
    "bridge.log_level": "log_level",
    "bridge.mc_confirm_window": "mc_confirm_window",
    "maintenance.mc_contact_max_age_days": "mc_contact_max_age_days",
    "maintenance.mt_node_max_age_days": "mt_node_max_age_days",
    "maintenance.maintenance_interval_hours": "maintenance_interval_hours",
    "maintenance.max_removals_per_run": "max_removals_per_run",
    "maintenance.health_grace_minutes": "health_grace_minutes",
    "maintenance.dry_run": "dry_run",
    "channels.mc_channel_name": "mc_channel_name",
    "channels.mc_channel_key": "mc_channel_key",
    "channels.mt_channel_name": "mt_channel_name",
    "channels.mt_channel_psk": "mt_channel_psk",
}


def parse_value(key, raw):
    """Prevede 'raw' (str z DB/formulare) na hodnotu dle schematu.

    Vrati (value, None) nebo (None, error). Nemuze hazet.
    """
    spec = CONFIG_SCHEMA[key]
    t = spec["type"]
    try:
        if t == "str":
            v = str(raw).strip()
            if not v:
                return None, "must not be empty"
            return v, None
        if t == "int":
            v = int(str(raw).strip())
        elif t == "float":
            v = float(str(raw).strip())
        elif t == "bool":
            if isinstance(raw, bool):
                return raw, None
            return str(raw).strip().lower() in ("1", "true", "yes", "on"), None
        elif t == "enum":
            v = str(raw).strip()
            if v not in spec["choices"]:
                return None, "must be one of " + "/".join(spec["choices"])
            return v, None
        elif t == "hex16":
            v = str(raw).strip().replace(" ", "").lower()
            if len(bytes.fromhex(v)) != 16:
                return None, "must be 16 bytes (32 hex chars)"
            return v, None
        elif t == "b64":
            import base64
            v = str(raw).strip()
            base64.b64decode(v)  # vyhodi ValueError, kdyz neni base64
            return v, None
        else:
            return None, f"unknown type {t}"
    except (ValueError, TypeError):
        return None, "invalid value"
    if "min" in spec and v < spec["min"]:
        return None, f"minimum is {spec['min']}"
    if "max" in spec and v > spec["max"]:
        return None, f"maximum is {spec['max']}"
    return v, None


def config_sections():
    """[(section, [key, ...])] v poradí definice schematu."""
    out = []
    for key, spec in CONFIG_SCHEMA.items():
        if not out or out[-1][0] != spec["section"]:
            out.append((spec["section"], []))
        out[-1][1].append(key)
    return out
