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
    "bridge.rate_limit_mc_to_mt": {
        "label": "Rate limit: min seconds between two MC→MT forwards (0 = off)",
        "type": "float", "min": 0, "max": 3600, "restart": False,
        "section": "Bridge"},
    "bridge.rate_limit_mt_to_mc": {
        "label": "Rate limit: min seconds between two MT→MC forwards (0 = off)",
        "type": "float", "min": 0, "max": 3600, "restart": False,
        "section": "Bridge"},
    "bridge.log_retention_days": {
        "label": "Auto-prune log entries older than (days; 0 = keep forever)",
        "type": "int", "min": 0, "max": 3650, "restart": False,
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
    # ---------------- shared channel setup (create_channel.py) ----------------
    "channels.mc_channel_name": {
        "label": "Shared channel: MeshCore name (lowercase; setup + "
                 "credentials panel, NOT the bridging pairs)",
        "type": "str", "restart": False, "section": "Shared channel setup"},
    "channels.mc_channel_key": {
        "label": "Shared channel: MeshCore key (16-byte hex) -- SECRET",
        "type": "hex16", "restart": False, "secret": True,
        "section": "Shared channel setup"},
    "channels.mt_channel_name": {
        "label": "Shared channel: Meshtastic name (setup only)",
        "type": "str", "restart": False, "section": "Shared channel setup"},
    "channels.mt_channel_psk": {
        "label": "Shared channel: Meshtastic PSK (base64; AQ== = public "
                 "default; setup only)",
        "type": "b64", "restart": False, "section": "Shared channel setup"},
    # ---------------- MQTT ----------------
    "mqtt.enabled": {
        "label": "MQTT client enabled",
        "type": "bool", "restart": False, "section": "MQTT"},
    "mqtt.host": {
        "label": "MQTT broker host (IP/hostname)",
        "type": "str", "restart": False, "section": "MQTT"},
    "mqtt.port": {
        "label": "MQTT broker port",
        "type": "int", "min": 1, "max": 65535, "restart": False,
        "section": "MQTT"},
    "mqtt.user": {
        "label": "MQTT username (empty = none)",
        "type": "str", "restart": False, "section": "MQTT"},
    "mqtt.password": {
        "label": "MQTT password",
        "type": "str", "restart": False, "secret": True, "section": "MQTT"},
    "mqtt.topic_prefix": {
        "label": "MQTT topic prefix",
        "type": "str", "restart": False, "section": "MQTT"},
    "mqtt.allow_inbound": {
        "label": "Accept MQTT <prefix>/send messages into the mesh",
        "type": "bool", "restart": False, "section": "MQTT"},
    "mqtt.hass_discovery": {
        "label": "Publish Home Assistant MQTT discovery (entities auto-appear)",
        "type": "bool", "restart": False, "section": "MQTT"},
    "mqtt.publish_nodes": {
        "label": "Publish node-info/telemetry topics (mc/self, mt/nodes/<id>)",
        "type": "bool", "restart": False, "section": "MQTT"},
    # ---------------- forwarding (opt-in) ----------------
    "forward.position_mc_to_mt": {
        "label": "Forward the MeshCore node's position to Meshtastic "
                 "(sendPosition; on change or every 6 h)",
        "type": "bool", "restart": False, "section": "Forwarding (opt-in)"},
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
    "bridge.rate_limit_mc_to_mt": "rate_limit_mc_to_mt",
    "bridge.rate_limit_mt_to_mc": "rate_limit_mt_to_mc",
    "bridge.log_retention_days": "log_retention_days",
    "mqtt.enabled": "mqtt_enabled",
    "mqtt.host": "mqtt_host",
    "mqtt.port": "mqtt_port",
    "mqtt.user": "mqtt_user",
    "mqtt.password": "mqtt_password",
    "mqtt.topic_prefix": "mqtt_topic_prefix",
    "mqtt.allow_inbound": "mqtt_allow_inbound",
    "mqtt.hass_discovery": "mqtt_hass_discovery",
    "mqtt.publish_nodes": "mqtt_publish_nodes",
    "forward.position_mc_to_mt": "forward_position_mc_to_mt",
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
