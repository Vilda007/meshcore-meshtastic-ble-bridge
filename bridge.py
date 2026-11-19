#!/usr/bin/env python3
"""MeshCore <-> Meshtastic BLE bridge.

Bezi na Raspberry Pi (nebo jinem linuxu s Bluetooth). Pripoji se pres BLE
k MeshCore zarizeni (companion protokol, implementace v meshcore_ble.py)
a k Meshtastic uzlu (knihovna meshtastic, BLEInterface) a prenasi textove
zpravy:

  - zprava z MeshCore kanalu  -> MeshTastic s prefixem "MC: "
  - zprava z MeshTastic kanalu -> MeshCore s prefixem "MT: "

Ochrany: zpravy uz zacavajici nekterym z prefixu se neprenasi (ochrana proti
zacykleni), LRU dedup cache, chunking na limit protistrany, auto-reconnect
na obou stranach.

Udrzba (1x denne): promazava dlouho neaktivni kontakty v MeshCore a stare
uzly v nodeDB Meshtastic (favoriti se nikdy nemazou). Zpravy se na zadnem
ze zarizeni nehromadi (MeshCore: RAM fronta max 16 zprav s FIFO prepisem;
Meshtastic: bez store_forward modulu zadna perzistence).

Usage: bridge.py [config.ini]
"""

import asyncio
import configparser
import hashlib
import json
import logging
import logging.handlers
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from collections import OrderedDict

from pubsub import pub

from config_schema import CONFIG_ATTR, CONFIG_SCHEMA, parse_value
from meshcore_ble import MeshCoreClient, PAYLOAD_TYPE_GRP_TXT

try:
    from meshtastic.ble_interface import BLEInterface
except ImportError:  # nezname BLE zarizeni az v runtime, ale knihovna chyti hned
    BLEInterface = None

try:
    import paho.mqtt.client as mqtt
except ImportError:  # MQTT strana je volitelna
    mqtt = None

log = logging.getLogger("bridge")


def sd_notify(msg):
    """systemd sd_notify (READY/WATCHDOG) bez externi knihovny.

    Zachycuje NOTIFY_SOCKET (abstraktni socket znamena '@' -> \\0); kdyz
    bridge bezne mimo systemd, neni co delat -- tise.
    """
    addr = os.getenv("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try:
            s.connect(addr)
            s.sendall(msg.encode())
        finally:
            s.close()
    except OSError:
        pass

# Meshtastic BLE volani (sendText, setTime, close, BLEInterface(...)) nemaji
# vlastni timeouty a po tichem umrti GATT spojeni (callback connection.lost
# nevznikne!) blokuji navzdy. Vsechny maji proto wait_for clenky; po timeoutu
# se spojeni oznaci za rozbite -> reconnect a zprava pujde do fronty.
MT_OP_TIMEOUT = 30        # jednotlive operace (sendText, setTime, cteni kanalu)
MT_CONNECT_TIMEOUT = 120   # pripojeni vc. BLE skenu (~10 s) a handshake


# ----------------------------------------------------------------- konfigurace

class Config:
    """Nactena konfigurace z config.ini (vc. defaultu)."""

    def __init__(self, path):
        ini = configparser.ConfigParser()
        if os.path.exists(path):
            ini.read(path, encoding="utf-8")
        else:
            log.warning("Konfigurace %s nenalezena, pouzivam defaulty.", path)

        mc = ini["meshcore"] if ini.has_section("meshcore") else {}
        mt = ini["meshtastic"] if ini.has_section("meshtastic") else {}
        br = ini["bridge"] if ini.has_section("bridge") else {}
        ud = ini["maintenance"] if ini.has_section("maintenance") else {}

        # MeshCore
        self.mc_device = mc.get("device") or None  # empty = first found
        self.mc_channel = mc.getint("channel", 0)
        self.mc_poll_interval = mc.getfloat("poll_interval", 30.0)

        # Meshtastic
        self.mt_device = mt.get("device") or None  # empty = first found
        self.mt_channel = mt.getint("channel", 0)

        # bridge
        self.prefix_to_mt = br.get("prefix_to_mt", "MC:").strip(' "')
        self.prefix_to_mc = br.get("prefix_to_mc", "MT:").strip(' "')
        # limit tela zpravy (bez prefixu); MeshCore ~133 znaku, MT ~233
        self.mc_max_len = br.getint("mc_max_len", 120)
        self.mt_max_len = br.getint("mt_max_len", 200)
        # rate limit: min. sekund mezi dvema preposlanymi zpravami (0 = vyp)
        self.rate_limit_mc_to_mt = br.getfloat("rate_limit_mc_to_mt", 0.0)
        self.rate_limit_mt_to_mc = br.getfloat("rate_limit_mt_to_mc", 0.0)
        self.dedup_cache = br.getint("dedup_cache", 400)
        # automaticke promazani zaznamu ve feedu/statistikach (0 = navzdy)
        self.log_retention_days = br.getint("log_retention_days", 0)
        self.log_level = br.get("log_level", "INFO").upper()
        self.log_file = br.get("log_file", "")  # prazdne = jen konzola/journal
        # dashboard: JSONL event log + status soubor (relativni = vedle configu)
        self.event_log = br.get("event_log", "events.jsonl")
        self.status_file = br.get("status_file", "status.json")
        # SQLite perzistence: zpravy + udalosti (statistiky, feed dashboardu)
        self.db_file = br.get("db_file", "stats.db")
        # dashboard: runtime-editovatelne mapovani kanalu + cache jmen kanalu
        self.mappings_file = br.get("mappings_file", "mappings.json")
        self.channels_file = br.get("channels_file", "channels.json")
        self.base_dir = os.path.dirname(os.path.abspath(path))
        # repeat-echo okno pro potvrzeni doruceni na MC strane (s; 0 = vyp)
        self.mc_confirm_window = br.getint("mc_confirm_window", 30)
        # sdileny kanal (create_channel.py); klic je TAJNY
        chs = ini["channels"] if ini.has_section("channels") else {}
        self.mc_channel_name = chs.get("mc_channel_name",
                                       "#meshbridge").strip()
        self.mc_channel_key = chs.get("mc_channel_key", "").strip()
        self.mt_channel_name = chs.get("mt_channel_name",
                                       self.mc_channel_name).strip()
        self.mt_channel_psk = chs.get("mt_channel_psk", "AQ==").strip()

        # MQTT (home automation / web klienty; volitelna strana)
        mq = ini["mqtt"] if ini.has_section("mqtt") else {}
        self.mqtt_enabled = mq.getboolean("enabled", False)
        self.mqtt_host = mq.get("host", "").strip()
        self.mqtt_port = mq.getint("port", 1883)
        self.mqtt_user = mq.get("user", "").strip()
        self.mqtt_password = mq.get("password", "")
        self.mqtt_topic_prefix = (mq.get("topic_prefix", "meshbridge")
                                  .strip() or "meshbridge")
        self.mqtt_allow_inbound = mq.getboolean("allow_inbound", False)
        self.mqtt_hass_discovery = mq.getboolean("hass_discovery", True)
        self.mqtt_publish_nodes = mq.getboolean("publish_nodes", True)

        # opt-in preposilani pozice/node-info (sekce [forward])
        fw = ini["forward"] if ini.has_section("forward") else {}
        self.forward_position_mc_to_mt = fw.getboolean("position_mc_to_mt",
                                                       False)

        # prekryti z SQLite (tabulka 'config' ve stats.db; ridi ji dashboard
        # Config stranka) -- ini zustava bootstrapem a zdrojem defaultu
        db_path = self.db_file
        if db_path and not os.path.isabs(db_path):
            db_path = os.path.join(self.base_dir, db_path)
        overlay = {}
        if db_path:
            try:
                conn = sqlite3.connect(db_path)
                overlay = {r[0]: r[1] for r in
                           conn.execute("SELECT key, value FROM config")}
                conn.close()
            except sqlite3.Error:
                overlay = {}
        self._apply_overlay(overlay)

        # mapovani kanalu MC -> MT: [channels] mappings = "mc:mt, mc:mt".
        # Neni-li sekce, pouzije se klasicka dvojice meshcore.channel a
        # meshtastic.channel (zpetna kompatibilita).
        raw_map = (ini["channels"].get("mappings", "")
                   if ini.has_section("channels") else "")
        self.mappings = Config.parse_mappings(
            raw_map or f"{self.mc_channel}:{self.mt_channel}")

        # udrzba
        ud = ud  # noqa
        self.mc_contact_max_age_days = (
            ini["maintenance"].getint("mc_contact_max_age_days", 14)
            if ini.has_section("maintenance") else 14)
        self.mt_node_max_age_days = (
            ini["maintenance"].getint("mt_node_max_age_days", 7)
            if ini.has_section("maintenance") else 7)
        self.maintenance_interval_hours = (
            ini["maintenance"].getfloat("maintenance_interval_hours", 24)
            if ini.has_section("maintenance") else 24.0)
        self.max_removals_per_run = (
            ini["maintenance"].getint("max_removals_per_run", 10)
            if ini.has_section("maintenance") else 10)
        # health self-check: udruzba musi dobehnout do interval+grace
        self.health_grace_minutes = (
            ini["maintenance"].getint("health_grace_minutes", 90)
            if ini.has_section("maintenance") else 90)
        self.dry_run = (
            ini["maintenance"].getboolean("dry_run", False)
            if ini.has_section("maintenance") else False)

    def _apply_overlay(self, values):
        """Prekryje konfiguraci hodnotami z DB (klice dle schematu).

        Neplatne hodnoty jen zaloguje a preskoci. Vrati list zmenenych klicu.
        """
        changed = []
        for key, raw in values.items():
            spec = CONFIG_SCHEMA.get(key)
            attr = CONFIG_ATTR.get(key)
            if not (spec and attr and raw is not None):
                continue
            val, err = parse_value(key, raw)
            if err or val is None:
                log.warning("Config: %s ma neplatnou hodnotu %r (%s)",
                            key, raw, err)
                continue
            if getattr(self, attr, None) != val:
                setattr(self, attr, val)
                changed.append(key)
        return changed

    @staticmethod
    def parse_mappings(raw):
        """'0:0, 1:2' -> [(0, 0), (1, 2)]; spatne dvojice se preskoci."""
        out = []
        for part in raw.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                mc, mt = part.split(":")
                out.append((int(mc.strip()), int(mt.strip())))
            except ValueError:
                log.warning("Neplatna dvojice mapovani kanalu: %r "
                            "(ocekavan tvar 'mc:mt')", part)
        return out


def setup_logging(cfg):
    fmt = "%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    logging.basicConfig(level=cfg.log_level, format=fmt)
    if cfg.log_file:
        os.makedirs(os.path.dirname(cfg.log_file) or ".", exist_ok=True)
        fh = logging.handlers.RotatingFileHandler(
            cfg.log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
        fh.setFormatter(logging.Formatter(fmt))
        logging.getLogger().addHandler(fh)


class EventLog:
    """JSONL log udalosti pro dashboard (prijem, preposlani, stavy).

    Drzi jen poslednich max_entries zaznamu -- dlouha historie neni potreba
    (rotace kontroluje se na startu a pak kazdych 25 zapisu).
    """

    def __init__(self, path, max_entries=150):
        self.path = path
        self.max_entries = max_entries
        self._writes = 0
        self._lock = asyncio.Lock()

    async def log(self, event, **kw):
        rec = {"ts": int(time.time() * 1000) / 1000.0, "event": event}
        rec.update(kw)
        line = json.dumps(rec, ensure_ascii=False)
        async with self._lock:
            try:
                with open(self.path, "a", encoding="utf-8") as f:
                    f.write(line + "\n")
                self._writes += 1
                if self._writes % 25 == 0:
                    await self.trim()
            except OSError as e:
                log.debug("event log write failed: %r", e)

    async def trim(self):
        """Presahuje-li soubor max_entries radku, necha jen poslednich."""
        try:
            with open(self.path, encoding="utf-8") as f:
                lines = f.readlines()
            if len(lines) > self.max_entries:
                tmp = self.path + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    f.writelines(lines[-self.max_entries:])
                os.replace(tmp, self.path)
        except OSError:
            pass


class BridgeDB:
    """SQLite perzistence: jednotny feed udalosti a zprav pro dashboard.

    Jedina tabulka 'log'; radky jsou vsechno, co projde _elog() (prijem,
    preposlani, fronty, connect/disconnect, udrzba...). Statistiky
    (zpravy/den, reconnecty, uptime) dashboard pocita SQL agregacemi.
    """

    def __init__(self, path):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS log ("
            " ts REAL NOT NULL, kind TEXT NOT NULL, direction TEXT,"
            " src TEXT, text TEXT, extra TEXT)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_log_ts ON log(ts)")
        self.conn.commit()

    def log(self, kind, direction=None, src=None, text=None, extra=None):
        """Vlozi radek (extra = dict -> JSON; chyby jen logne)."""
        try:
            self.conn.execute(
                "INSERT INTO log (ts, kind, direction, src, text, extra)"
                " VALUES (?,?,?,?,?,?)",
                (time.time(), kind, direction, src, text,
                 json.dumps(extra, ensure_ascii=False) if extra else None))
            self.conn.commit()
        except sqlite3.Error as e:
            log.debug("sqlite write failed: %r", e)

    def close(self):
        try:
            self.conn.close()
        except sqlite3.Error:
            pass


class MqttSide:
    """MQTT vystup/vstup pro home-automation a web klienty (paho-mqtt).

    Temata pod topic_prefix (jednoduchy JSON, ne LetsMesh observer format):
      <p>/mc/recv    {"ts","channel","text"}       kazda zprava z MeshCore
      <p>/mt/recv    {"ts","channel","src","text"} kazda zprava z Meshtastic
      <p>/status     {mc,mt spojeni, statistiky}    retained, co 60 s
    Pri allow_inbound=True odbera <p>/send:
      {"side": "mc"|"mt", "channel": <idx>, "text": "..."} -> do site.
    Bez knihovny paho-mqtt / vypnuteho enabled je cela strana no-op.
    """

    def __init__(self, cfg, on_inbound):
        self.cfg = cfg
        self.on_inbound = on_inbound          # async fn(mqtt.MQTTMessage)
        self.loop = None
        self.client = None
        self.connected = False

    def start(self):
        self.stop()
        if not (mqtt and self.cfg.mqtt_enabled and self.cfg.mqtt_host):
            if self.cfg.mqtt_enabled and not mqtt:
                log.warning("MQTT: chybi knihovna paho-mqtt "
                            "(pip install paho-mqtt) -- strana vypnuta")
            return False
        try:  # paho 2.x; fallback pro 1.x
            c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                            client_id="mesh-bridge")
        except (AttributeError, TypeError):
            c = mqtt.Client(client_id="mesh-bridge")
        if self.cfg.mqtt_user:
            c.username_pw_set(self.cfg.mqtt_user, self.cfg.mqtt_password or None)
        p = self.cfg.mqtt_topic_prefix
        # LWT: az bridge spadne/ho nekdo zastavi, HA entity prejdou na unavailable
        c.will_set(f"{p}/availability", "offline", retain=True)
        c.on_connect = self._on_connect
        c.on_disconnect = self._on_disconnect
        c.on_message = self._on_message
        c.reconnect_delay_set(1, 60)
        try:
            c.connect_async(self.cfg.mqtt_host, self.cfg.mqtt_port, 30)
            c.loop_start()
        except Exception as e:
            log.warning("MQTT: pripojeni k %s:%d selhalo: %r",
                        self.cfg.mqtt_host, self.cfg.mqtt_port, e)
            return False
        self.client = c
        log.info("MQTT: pripojuji k %s:%d (prefix %r)",
                 self.cfg.mqtt_host, self.cfg.mqtt_port,
                 self.cfg.mqtt_topic_prefix)
        return True

    def stop(self):
        c, self.client = self.client, None
        self.connected = False
        if c:
            try:
                c.disconnect()
            except Exception:
                pass
            try:
                c.loop_stop()
            except Exception:
                pass

    def restart(self):
        if self.cfg.mqtt_enabled:
            self.start()

    def publish(self, subtopic, payload, retain=False):
        """Nejvyssi uroven pro Bridge._mqtt_pub (neblokuje, vlastni vlakno)."""
        if self.client and self.connected:
            try:
                if isinstance(payload, str):
                    body = payload          # plain retezec bez JSON uvozovek
                else:
                    body = json.dumps(payload, ensure_ascii=False)
                self.client.publish(
                    f"{self.cfg.mqtt_topic_prefix}/{subtopic}",
                    body, retain=retain, qos=0)
            except Exception:
                pass

    def publish_topic(self, topic, payload, retain=False):
        """Publish na cely topic BEZ topic_prefix (HA discovery)."""
        if self.client and self.connected:
            try:
                if isinstance(payload, str):
                    body = payload
                else:
                    body = json.dumps(payload, ensure_ascii=False)
                self.client.publish(topic, body, retain=retain, qos=0)
            except Exception:
                pass

    # ---------------------------------------------------------- paho callbacky

    @staticmethod
    def _rc_ok(rc):
        if isinstance(rc, int):          # paho 1.x
            return rc == 0
        return not getattr(rc, "is_failure", True)   # paho 2.x ReasonCode

    def _on_connect(self, client, userdata, flags, rc, *rest):
        if self._rc_ok(rc):
            self.connected = True
            log.info("MQTT: pripojeno")
            p = self.cfg.mqtt_topic_prefix
            client.publish(f"{p}/availability", "online", retain=True)
            client.subscribe(f"{p}/cmd")   # ovladaci prikazy (restart)
            if self.cfg.mqtt_allow_inbound:
                client.subscribe(f"{p}/send")
                log.info("MQTT: odber %s/send (inbound do site)", p)
        else:
            log.warning("MQTT: broker odmitl spojeni (rc=%s)", rc)

    def _on_disconnect(self, *args):
        was = self.connected
        self.connected = False
        if was:
            log.warning("MQTT: spojeni ztraceno (paho zkusi reconnect)")

    def _on_message(self, client, userdata, msg):
        if self.on_inbound and self.loop:
            asyncio.run_coroutine_threadsafe(self.on_inbound(msg), self.loop)


# --------------------------------------------------------------------- bridge

class Bridge:
    def __init__(self, cfg):
        self.cfg = cfg
        self.loop = None
        self._running = True

        ev_path = cfg.event_log
        if ev_path and not os.path.isabs(ev_path):
            ev_path = os.path.join(cfg.base_dir, ev_path)
        self.events = EventLog(ev_path) if ev_path else None
        # SQLite perzistence (feed + statistiky); selhani neni fatalni
        db_path = cfg.db_file
        if db_path and not os.path.isabs(db_path):
            db_path = os.path.join(cfg.base_dir, db_path)
        self.db = None
        if db_path:
            try:
                self.db = BridgeDB(db_path)
                self.db.log("bridge_start")
            except sqlite3.Error as e:
                log.warning("SQLite %s nelze otevrit: %r", db_path, e)
        st_path = cfg.status_file
        if st_path and not os.path.isabs(st_path):
            st_path = os.path.join(cfg.base_dir, st_path)
        self.status_path = st_path
        self._last_mc_state = None

        # mapovani kanalu: vychozi z configu; z DB ('channels.mappings',
        # ridi dashboard) ma prednost; legacy mappings.json se jednorazove
        # premigruje. Zmeny hlida flush smycka (do 15 s).
        self.mappings = list(cfg.mappings)
        self.mappings_path = os.path.join(cfg.base_dir, cfg.mappings_file)
        self.channels_path = os.path.join(cfg.base_dir, cfg.channels_file)
        self._channels_done = False  # po pripojeni stran zjistime jmena kanalu
        self._config_updated = -1.0
        rows, mappings, updated = self._db_config()
        if mappings is None:
            mappings = self._migrate_mappings_json()
        if mappings is not None:
            self.mappings = mappings
        if updated is not None:
            self._config_updated = updated

        self.mc = None            # MeshCoreClient
        self.mt = None            # meshtastic BLEInterface
        self.my_id = None         # "!xxxxxxxx" naseho MT uzlu

        # systemd watchdog + health self-check
        self._watchdog_healthy = True
        self._last_maintenance_ok = time.time()

        # potvrzeni doruceni na MC strane: ciphertext nasi zpravy ->
        # {deadline, ts, sent_at, text, pre}; exspirace proleze flush loopa
        self._mc_secrets = {}       # mc kanal -> 16bajtovy klic (cache)
        self._mc_cts = OrderedDict()
        self._last_fwd_mt = 0.0     # rate limit (monotonic)
        self._last_fwd_mc = 0.0

        self.mqtt = None            # MqttSide (volitelna MQTT strana)

        self.mt_lost = asyncio.Event()
        self.mt_send_lock = asyncio.Lock()
        self.seen = OrderedDict()  # dedup LRU: key -> monotonicky cas
        self.mt_pending = []       # MC->MT zpravy cekajici na pripojeni MT
        self.mc_pending = []       # MT->MC zpravy cekajici na pripojeni MC
        self.stats = {"mc_to_mt": 0, "mt_to_mc": 0,
                      "mc_contacts_removed": 0, "mt_nodes_removed": 0,
                      "mc_dropped": 0, "mt_dropped": 0,
                      "queued": 0, "flushed": 0, "mc_repeated": 0}

    # -------------------------------------------------------------- hlavni beh

    async def run(self):
        self.loop = asyncio.get_running_loop()
        if self.events:
            await self.events.trim()  # hned zkrotit pripadny stary dlouhy log

        # meshtastic knihovna vola callbacky z vlastnich vlaken
        pub.subscribe(self._on_mt_receive, "meshtastic.receive")
        pub.subscribe(self._on_mt_lost, "meshtastic.connection.lost")

        self.mc = MeshCoreClient(
            self._on_mc_message,
            name_filter=self.cfg.mc_device,
            poll_interval=self.cfg.mc_poll_interval,
            app_name="mesh-bridge",
            on_raw_rx=self._on_mc_raw_rx)

        # MQTT (volitelne; start je no-op, kdyz neni enabled/knihovna)
        self.mqtt = MqttSide(self.cfg, self._on_mqtt_inbound)
        self.mqtt.loop = asyncio.get_running_loop()
        self.mqtt.start()

        async def mc_start():
            # pockat, az se pripoji Meshtastic (BLE skeny obou klientu
            # na jednom adapteru se navzajem rusi)
            for _ in range(20):
                if self.mt or not self._running:
                    break
                await asyncio.sleep(3)
            await self.mc.run()

        tasks = [
            asyncio.create_task(mc_start(), name="meshcore"),
            asyncio.create_task(self._mt_loop(), name="meshtastic"),
            asyncio.create_task(self._maintenance_loop(), name="maintenance"),
            asyncio.create_task(self._pending_flush_loop(), name="pending"),
            asyncio.create_task(self._status_loop(), name="status"),
            asyncio.create_task(self._watchdog_loop(), name="watchdog"),
            asyncio.create_task(self._mqtt_status_loop(), name="mqtt-status"),
        ]
        sd_notify("READY=1")   # systemd: unita Type=notify je "started"
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.shutdown()

    async def shutdown(self):
        self._running = False
        self.mt_lost.set()
        if self.mc:
            await self.mc.stop()
        if self.mt:
            try:
                await asyncio.wait_for(
                    self.loop.run_in_executor(None, self.mt.close), 10)
            except Exception:
                pass
        if self.mqtt:
            self.mqtt.stop()
        if self.db:
            self.db.log("bridge_stop")   # hranice uptime v statistikach
            self.db.close()
        log.info("Bridge zastaven (MC->MT %d, MT->MC %d)",
                 self.stats["mc_to_mt"], self.stats["mt_to_mc"])

    # -------------------------------------------------------------- meshtastic

    def _mt_broken(self, why):
        """Oznaci MT spojeni za mrtve a vynuti reconnect.

        Volano po timeoutu operace (zavisle GATT spojeni) nebo selhani
        sendTextu. close() muze take blokovat, proto bezi na pozadi bez
        cekani; nespousti se v event loop, takze ji nemuze zaseknout.
        """
        iface, self.mt = self.mt, None
        self.mt_lost.set()
        log.warning("MT: %s -- spojeni oznaceno za rozbite, reconnect", why)
        if iface is not None:
            def _close():
                try:
                    iface.close()
                except Exception:
                    pass
            self.loop.run_in_executor(None, _close)

    def _mt_connect(self):
        """Pripojeni BLEInterface; bezi v executoru (blokuje). Vraci iface."""
        if self.mt:
            try:
                self.mt.close()
            except Exception:
                pass
            self.mt = None
        address = self.cfg.mt_device or None
        log.info("MT: pripojuji k %s...", address or "prvniho nalezeneho")
        iface = BLEInterface(address)
        try:
            self.my_id = f"!{iface.myInfo.my_node_num:08x}"
        except Exception:
            self.my_id = None
        # citelne jmeno zarizeni (pro dashboard/HA status)
        try:
            self.mt_node_name = iface.getLongName() or self.my_id
        except Exception:
            self.mt_node_name = self.my_id
        log.info("MT: pripojeno (self=%s)", self.my_id)
        # hodiny: Meshtastic nema cteni casu, nastavime rovnou
        try:
            iface.localNode.setTime()
            log.info("MT: hodiny zarizeni synchronizovany na systemovy cas")
        except Exception as e:
            log.warning("MT: nastaveni hodin selhalo: %r", e)
        return iface

    @staticmethod
    def _mt_orphan_close(fut):
        """Zavre iface z opozdene dokonceneho pripojeni (timeout ho zahodil).

        Vlakno pripojeni muze doběhnout i po timeoutu wait -- jeho GATT
        spojeni by viselo dal, proto ho jakmile dobehne zavreme.
        """
        try:
            iface = fut.result()
            if iface is not None:
                iface.close()
        except Exception:
            pass

    async def _mt_loop(self):
        backoff = 5
        while self._running:
            try:
                fut = self.loop.run_in_executor(None, self._mt_connect)
                done, pending = await asyncio.wait(
                    {fut}, timeout=MT_CONNECT_TIMEOUT)
                if pending:
                    fut.add_done_callback(self._mt_orphan_close)
                    raise TimeoutError(
                        f"MT: pripojeni timeout (>{MT_CONNECT_TIMEOUT} s)")
                self.mt = fut.result()
                backoff = 5
                await self._elog("connect", side="mt")
                self.write_status()
                log.info("MT: spojeni OK, flush fronty (%d zprav)",
                         len(self.mt_pending))
                while self.mt_pending and self.mt:
                    text, ch = self.mt_pending.pop(0)
                    ok = await self._mt_send(text, ch, queue_on_fail=False)
                    if not ok:
                        self.mt_pending.insert(0, text)  # vrat zpet
                        break  # spojeni zase prasklo; zbytek ceka dál
                    self.stats["flushed"] += 1
                self.mt_lost.clear()
                await self.mt_lost.wait()
                if not self._running:
                    break
                log.warning("MT: spojeni ztraceno")
                await self._elog("disconnect", side="mt")
                self.write_status()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("MT: pripojeni selhalo: %r", e)
                self.mt = None
                self._bluez_cleanup_mt()
            if not self._running:
                break
            log.info("MT: novy pokus za %d s", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120)

    def _bluez_cleanup_mt(self):
        """Odpoji zavisle BLE spojeni k MT zarizeni.

        Po selhanem handshake muze BlueZ drzet GATT spojeni otevrene --
        zarizeni pak neinzeruje a zadny dalsi pokus ho nenajde.
        """
        import subprocess
        try:
            out = subprocess.run(["bluetoothctl", "devices"],
                                capture_output=True, text=True,
                                timeout=15).stdout
            needle = (self.cfg.mt_device or "").lower()
            for line in out.splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                mac = parts[1]
                if (needle and needle in line.lower()) or \
                        "meshtastic" in line.lower():
                    subprocess.run(["bluetoothctl", "disconnect", mac],
                                   capture_output=True, timeout=15)
                    log.info("MT: pomocne odpojeno zavisle spojeni %s", mac)
        except Exception as e:
            log.debug("MT: bluez cleanup selhal: %r", e)

    def _on_mt_lost(self, interface=None):
        """Callback z vlakna knihovny (topic meshtastic.connection.lost)."""
        if self.loop:
            self.loop.call_soon_threadsafe(self.mt_lost.set)

    def _on_mt_receive(self, packet, interface=None):
        """Callback z vlakna knihovny (topic meshtastic.receive)."""
        if self.loop:
            asyncio.run_coroutine_threadsafe(self._handle_mt(packet), self.loop)

    async def _handle_mt(self, packet):
        try:
            decoded = packet.get("decoded", {})
            if decoded.get("portnum") != "TEXT_MESSAGE":
                # opt-in: node-info/telemetry -> MQTT (HA feed)
                if self.cfg.mqtt_publish_nodes:
                    self._mt_nodeinfo_mqtt(packet, decoded)
                return
            text = decoded.get("text")
            if not text:
                return
            mc_ch = self._mc_for_mt(packet.get("channel", 0))
            if mc_ch is None:
                return  # kanal neni bridgovany
            from_id = packet.get("fromId")
            if self.my_id and from_id == self.my_id:
                return
            # ochrana proti zacykleni: obsah ktery uz byl prelozeny pres bridge
            if text.startswith(self.cfg.prefix_to_mt) or \
               text.startswith(self.cfg.prefix_to_mc):
                self.stats["mt_dropped"] += 1
                await self._elog("dropped", side="mt", reason="loop-guard",
                                 text=text[:120])
                return
            # MQTT feed (home automation) -- vsechny kanaly, i nebridgovane
            self._mqtt_pub("mt/recv", {
                "ts": time.time(), "channel": packet.get("channel", 0),
                "src": from_id, "text": text})
            pid = packet.get("id")
            key = (f"mt:{pid}" if pid
                   else "mt:" + hashlib.md5(
                       f"{from_id}{text}{time.time():.0f}".encode()
                   ).hexdigest())
            if self._dedup(key):
                await self._elog("dropped", side="mt", reason="dedup",
                                 text=text[:120])
                return

            log.info("MT->MC: %r (od %s)", text, from_id)
            await self._elog("recv", side="mt", src=from_id, text=text[:200])
            body_limit = self.cfg.mc_max_len - len(self.cfg.prefix_to_mc) - 1
            for chunk in self._chunks(text, body_limit):
                msg = f"{self.cfg.prefix_to_mc} {chunk}"
                res = await self._mc_send(msg, mc_ch)
                if res["ok"]:
                    self.stats["mt_to_mc"] += 1
                    await self._elog("forward", direction="mt_to_mc",
                                     text=msg[:200])
                    await self._mc_track_delivery(msg, res)
                else:
                    self._enqueue(self.mc_pending, msg, "MC")
                    log.warning("MT->MC: odeslani selhalo, ve fronte: %r",
                                chunk)
        except Exception:
            log.exception("MT: chyba pri zpracovani paketu")

    async def _mt_send(self, text, channel_idx, queue_on_fail=True):
        """sendText na dany kanal v executoru; selhani/timeout -> do fronty.

        Polozky fronty jsou dvojice (text, channel_idx).
        """
        await self._throttle("mt")   # rate limit MC->MT (vc. flush fronty)
        async with self.mt_send_lock:
            iface = self.mt
            if iface is not None:
                try:
                    await asyncio.wait_for(
                        self.loop.run_in_executor(
                            None,
                            lambda: iface.sendText(text,
                                                   channelIndex=channel_idx)),
                        MT_OP_TIMEOUT)
                    return True
                except asyncio.TimeoutError:
                    self._mt_broken(f"sendText timeout (>{MT_OP_TIMEOUT} s)")
                except Exception as e:
                    log.warning("MT: sendText selhal: %r", e)
                    self._mt_broken("sendText selhal")
            if queue_on_fail:
                self._enqueue(self.mt_pending, (text, channel_idx), "MT")
                await self._elog("queued", direction="mc_to_mt",
                                 text=text[:200])
            return False

    @staticmethod
    def _enqueue(queue, item, side, limit=50):
        """Bounded fronta pro preposlani po obnoveni spojeni.

        Polozky: MC strana (text, mc_kanal), MT strana (text, mt_kanal).
        """
        preview = item[0] if isinstance(item, tuple) else item
        if len(queue) >= limit:
            dropped = queue.pop(0)
            log.warning("%s: fronta plna, zahazuji nejnizsi: %r",
                        side, (dropped[0] if isinstance(dropped, tuple)
                               else dropped)[:50])
        queue.append(item)
        log.info("%s: zprava ceka ve fronte (%d): %r",
                 side, len(queue), preview[:50])

    async def _pending_flush_loop(self):
        """Pravidelne proleje fronty, az se strany pripoji.

        Take sleduje zmeny stavu spojeni (pro event log + status.json).
        """
        while self._running:
            await asyncio.sleep(15)
            try:
                self._check_config()
                await self._mc_confirm_sweep()
                mc_now = bool(self.mc and self.mc.connected)
                mt_now = bool(self.mt)
                if not (mc_now and mt_now):
                    self._channels_done = False  # po reconnectu zjistit znovu
                if mc_now != self._last_mc_state:
                    await self._elog("connect" if mc_now else "disconnect",
                                     side="mc")
                    self._last_mc_state = mc_now
                if mc_now and mt_now and not self._channels_done:
                    self._channels_done = True
                    await self._discover_channels()
                while self.mc_pending and self.mc and self.mc.connected:
                    text, ch = self.mc_pending.pop(0)
                    res = await self._mc_send(text, ch)
                    ok = res["ok"]
                    if ok:
                        self.stats["flushed"] += 1
                        await self._elog("flush", direction="mt_to_mc",
                                         text=text[:200])
                    else:
                        self.mc_pending.insert(0, (text, ch))
                        break
                while self.mt_pending and self.mt and \
                        not self.mt_lost.is_set():
                    text, ch = self.mt_pending.pop(0)
                    ok = await self._mt_send(text, ch, queue_on_fail=False)
                    if not ok:
                        self.mt_pending.insert(0, (text, ch))
                        break
                    self.stats["flushed"] += 1
                    await self._elog("flush", direction="mc_to_mt",
                                     text=text[:200])
                self.write_status()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("CHYBA: flush fronty")

    def write_status(self):
        """Status pro dashboard (atomic write; selhani jen logne)."""
        if not self.status_path:
            return
        try:
            st = {
                "updated": int(time.time()),
                "mc_connected": bool(self.mc and self.mc.connected),
                "mc_node": (self.mc.info or {}).get("name", self.cfg.mc_device),
                "mt_connected": bool(self.mt),
                "mt_node": getattr(self, "mt_node_name", self.my_id),
                "mappings": [list(p) for p in self.mappings],
                "stats": self.stats,
                "queue_mc": len(self.mc_pending),
                "queue_mt": len(self.mt_pending),
                "health_ok": self._watchdog_healthy,
                "last_maintenance": int(self._last_maintenance_ok),
                # konfigurace pro dashboard Config stranku (vc. kanalu =
                # atributu schematu; secret hodnoty vid jen admin pres API)
                "config": {key: getattr(self.cfg, attr, None)
                           for key, attr in CONFIG_ATTR.items()},
            }
            tmp = self.status_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False)
            os.replace(tmp, self.status_path)
        except OSError as e:
            log.debug("status write failed: %r", e)

    # --------------------------------------------------------------- meshcore

    async def _throttle(self, side):
        """Rate limit: min. pocet sekund mezi dvema preposlanimi zpravami.

        side = "mt" (MC->MT) / "mc" (MT->MC); limit 0 = vypnuto. Ceka pred
        odeslanim, at priliv zprav nezaplavi LoRa kanal.
        """
        limit = (self.cfg.rate_limit_mc_to_mt if side == "mt"
                 else self.cfg.rate_limit_mt_to_mc)
        if limit <= 0:
            return
        last_attr = "_last_fwd_" + side
        wait = getattr(self, last_attr, 0.0) + limit - time.monotonic()
        if wait > 0:
            log.info("RATE LIMIT (%s): cekam %.1f s pred odeslanim",
                     side, wait)
            await asyncio.sleep(wait)
        setattr(self, last_attr, time.monotonic())

    async def _mc_send(self, text, channel_idx):
        """Odeslani na MC stranu s rate limitem (pouziva ho i flush fronty).

        Vraci dict jako send_channel_message_confirm.
        """
        await self._throttle("mc")
        secret = await self._mc_secret(channel_idx)
        node_name = (self.mc.info or {}).get("name")
        return await self.mc.send_channel_message_confirm(
            channel_idx, text, channel_secret=secret, node_name=node_name)

    def _mqtt_pub(self, subtopic, payload, retain=False):
        """Neblokujici MQTT publish; bez MQTT strany je no-op."""
        if self.mqtt:
            self.mqtt.publish(subtopic, payload, retain=retain)

    async def _on_mqtt_inbound(self, msg):
        """Dispatch zprav z <prefix>/send a <prefix>/cmd."""
        prefix = self.cfg.mqtt_topic_prefix
        if msg.topic == f"{prefix}/cmd":
            await self._on_mqtt_cmd(msg)
            return
        """<prefix>/send: {"side":"mc"|"mt","channel":int,"text":str} -> sit."""
        try:
            data = json.loads(msg.payload.decode("utf-8", "replace"))
            side = str(data.get("side") or "").lower()
            text = str(data.get("text") or "").strip()
            channel = int(data.get("channel", 0))
        except (ValueError, TypeError, AttributeError,
                UnicodeDecodeError, json.JSONDecodeError):
            log.warning("MQTT: neplatna zprava na %s: %r",
                        msg.topic, msg.payload[:80])
            return
        if not text:
            log.warning("MQTT: prazdny text na %s -- ignoruji", msg.topic)
            return
        log.info("MQTT->%s (kanal %d): %r", side.upper(), channel, text[:60])
        await self._elog("mqtt", side=side, src="mqtt",
                         text=text[:200])
        if side == "mc" and self.mc and self.mc.connected:
            res = await self._mc_send(text, channel)
            if res["ok"]:
                self.stats["mt_to_mc"] += 1
                await self._elog("forward", direction="mt_to_mc",
                                 text=text[:200])
                await self._mc_track_delivery(text, res)
        elif side == "mt" and self.mt:
            ok = await self._mt_send(text, channel)
            if ok:
                self.stats["mc_to_mt"] += 1
                await self._elog("forward", direction="mc_to_mt",
                                 text=text[:200])
        else:
            log.warning("MQTT: neznama strana %r nebo strana nepripojena",
                        side)

    async def _on_mqtt_cmd(self, msg):
        """<prefix>/cmd: {"cmd":"restart"} -- restart bridge pres systemd."""
        try:
            data = json.loads(msg.payload.decode("utf-8", "replace"))
            cmd = str(data.get("cmd") or "").lower()
        except (ValueError, AttributeError, UnicodeDecodeError,
                json.JSONDecodeError):
            log.warning("MQTT: neplatny prikaz na %s: %r",
                        msg.topic, msg.payload[:80])
            return
        if cmd != "restart":
            log.warning("MQTT: neznamy prikaz %r (zname: restart)", cmd)
            return
        log.warning("MQTT: prikaz restart -- restartuji mesh-bridge")
        await self._elog("mqtt", side="cmd", src="mqtt", text="restart")
        try:
            subprocess.run(["sudo", "-n", "/usr/bin/systemctl", "--no-block",
                            "restart", "mesh-bridge"],
                           capture_output=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as e:
            log.warning("MQTT: restart selhal: %r", e)

    async def _mqtt_status_loop(self):
        """Retained status na <prefix>/status co 60 s (home automation).

        Pri zaplem hass_discovery publikuje i HA MQTT discovery (retained,
        idempotentni) -- entity bridge se v Home Assistant objevi samy.
        """
        while self._running:
            await asyncio.sleep(60)
            if not (self.mqtt and self.mqtt.connected):
                continue
            status = {
                "updated": int(time.time()),
                "mc_connected": bool(self.mc and self.mc.connected),
                "mc_node": (self.mc.info or {}).get("name",
                                                   self.cfg.mc_device),
                "mt_connected": bool(self.mt),
                "mt_node": getattr(self, "mt_node_name", self.my_id),
                "health_ok": self._watchdog_healthy,
                "mappings": [list(p) for p in self.mappings],
                "stats": self.stats,
                "queue_mc": len(self.mc_pending),
                "queue_mt": len(self.mt_pending),
            }
            self._mqtt_pub("status", status, retain=True)
            # binarni stavy a jmena zarzeni jako plain topicy (bez sablon);
            # POZOR: HA 2026+ MQTT binary_sensor default payload je
            # "ON"/"OFF" velkymi pismeny a porovnava presne!
            self._mqtt_pub("mc/connected",
                           "ON" if status["mc_connected"] else "OFF",
                           retain=True)
            self._mqtt_pub("mt/connected",
                           "ON" if status["mt_connected"] else "OFF",
                           retain=True)
            self._mqtt_pub("health",
                           "ON" if status["health_ok"] else "OFF",
                           retain=True)
            self._mqtt_pub("mc/node", status["mc_node"], retain=True)
            self._mqtt_pub("mt/node", status["mt_node"], retain=True)
            if self.cfg.mqtt_publish_nodes:
                self._publish_mc_self()
            await self._forward_mc_position()
            if self.cfg.mqtt_hass_discovery:
                self._mqtt_hass_discovery()

    def _mqtt_hass_discovery(self):
        """HA MQTT discovery na homeassistant/<typ>/<uid>/config (retained).

        Topic je pevny prefix 'homeassistant' (NE pod topic_prefix);
        vsechny entity maji availability_topic (LWT offline pri zastaveni).
        """
        p = self.cfg.mqtt_topic_prefix
        st = f"{p}/status"
        avail = f"{p}/availability"
        device = {"identifiers": ["meshbridge"], "name": "Mesh Bridge",
                  "manufacturer": "meshcore-meshtastic-ble-bridge"}
        items = [
            # binarni stavy: plain topicky (meshbridge/mc/connected = on/off),
            # bez value_template -- spolehlive na vsech verzich HA;
            # unique_id *_v2 -- HA pri "update" jiz existujici entity
            # neprepina jeji interni payload/template, cerstve unique_id
            # = vzdy funkcni entity objekt
            ("binary_sensor", "meshbridge_mc_v2", "MeshCore connected",
             f"{p}/mc/connected", "connectivity", None),
            ("binary_sensor", "meshbridge_mt_v2", "Meshtastic connected",
             f"{p}/mt/connected", "connectivity", None),
            ("binary_sensor", "meshbridge_health_v2", "Bridge health",
             f"{p}/health", None, None),
            ("sensor", "meshbridge_mc_node_v2", "MeshCore node",
             f"{p}/mc/node", None, None),
            ("sensor", "meshbridge_mt_node_v2", "Meshtastic node",
             f"{p}/mt/node", None, None),
            # cisla ze status JSON (templaty zde funguji)
            ("sensor", "meshbridge_mc_to_mt", "Forwarded MC→MT",
             st, None, "{{ value_json.stats.mc_to_mt }}"),
            ("sensor", "meshbridge_mt_to_mc", "Forwarded MT→MC",
             st, None, "{{ value_json.stats.mt_to_mc }}"),
            ("sensor", "meshbridge_mc_repeated", "Repeater echoes",
             st, None, "{{ value_json.stats.mc_repeated }}"),
            ("sensor", "meshbridge_queue_mc", "Queue MC",
             st, None, "{{ value_json.queue_mc }}"),
            ("sensor", "meshbridge_queue_mt", "Queue MT",
             st, None, "{{ value_json.queue_mt }}"),
            ("sensor", "meshbridge_last_mc", "Last MeshCore message",
             f"{p}/mc/recv", None, "{{ value_json.text }}"),
            ("sensor", "meshbridge_last_mt", "Last Meshtastic message",
             f"{p}/mt/recv", None, "{{ value_json.text }}"),
        ]
        for typ, uid, name, state_topic, dev_class, tpl in items:
            cfg = {"name": name, "unique_id": uid, "device": device,
                   "state_topic": state_topic,
                   "availability_topic": avail,
                   "payload_available": "online",
                   "payload_not_available": "offline"}
            if tpl:
                cfg["value_template"] = tpl
            if dev_class:
                cfg["device_class"] = dev_class
            self.mqtt.publish_topic(f"homeassistant/{typ}/{uid}/config",
                                    cfg, retain=True)

    async def _mc_secret(self, mc_idx):
        """16bajtovy klic MC kanalu (cache; None = neznamy/prazdny slot)."""
        if mc_idx not in self._mc_secrets:
            ch = None
            if self.mc and self.mc.connected:
                try:
                    ch = await asyncio.wait_for(self.mc.get_channel(mc_idx),
                                                15)
                except asyncio.TimeoutError:
                    pass
            self._mc_secrets[mc_idx] = (bytes.fromhex(ch["secret"])
                                        if ch and ch.get("secret") else None)
        return self._mc_secrets[mc_idx]

    async def _mc_track_delivery(self, msg, res):
        """Zaeviduje cekani na repeat-echo nasi zpravy (MC strana).

        Ocekavany ciphertext je deterministicky (AES-128-ECB bez IV), takze
        echo od repeateru lze spoznat v raw RX snifferu bajtove presne.
        """
        if not (res.get("ct") and self.cfg.mc_confirm_window > 0):
            return
        self._mc_cts[res["ct"].hex()] = {
            "deadline": time.monotonic() + self.cfg.mc_confirm_window,
            "sent_at": time.monotonic(), "ts": res["ts"],
            "text": msg[:80], "pre": res.get("pre")}

    async def _on_mc_raw_rx(self, pkt):
        """Sniffer vsech prijatych raw packetu -> repeat-echo nasi zpravy.

        GRP_TXT payload: [channel_hash (1)][MAC (2)][ciphertext]. Nas
        ciphertext zname -- porovnani je exaktni, cizi provoz neresifrujeme.
        """
        try:
            if pkt["payload_type"] != PAYLOAD_TYPE_GRP_TXT:
                return
            tail = bytes(pkt["payload"][3:])
            if not tail:
                return
            rec = self._mc_cts.pop(tail.hex(), None)
            if not rec:
                return
            delay = time.monotonic() - rec["sent_at"]
            self.stats["mc_repeated"] += 1
            log.info("MC: repeat-echo nasi zpravy po %.1f s "
                     "(SNR %.1f dB, RSSI %d dBm): %r", delay,
                     pkt["snr"], pkt["rssi"], rec["text"])
            await self._elog("mc_repeat", snr=pkt["snr"], rssi=pkt["rssi"],
                             delay_s=round(delay, 1), text=rec["text"])
        except Exception:
            log.exception("MC: chyba pri zpracovani repeat-echo")

    async def _mc_confirm_sweep(self):
        """Expirovana cekani na echo: vypocet TX-statistikou potvrzeni."""
        now = time.monotonic()
        for ct_hex in [k for k, r in self._mc_cts.items()
                       if now >= r["deadline"]]:
            rec = self._mc_cts.pop(ct_hex)
            mc_tx = None
            if self.mc and self.mc.connected and rec.get("pre"):
                try:
                    post = await asyncio.wait_for(self.mc.get_packet_stats(),
                                                  10)
                except asyncio.TimeoutError:
                    post = None
                if post:
                    # citac 'sent' roste jen skutecnym vysilanim; jine
                    # flood pakety (adverty) ho ale take posouvaji
                    mc_tx = post.get("sent", 0) > rec["pre"].get("sent", 0)
            log.info("MC: zadne repeat-echo do %d s (tx=%s): %r",
                     self.cfg.mc_confirm_window, mc_tx, rec["text"])
            await self._elog("mc_confirm", mc_tx=mc_tx,
                             text=rec["text"])

    async def _on_mc_message(self, text, channel_idx, ts):
        try:
            # MQTT feed (home automation) -- vsechny kanaly, i nebridgovane
            self._mqtt_pub("mc/recv", {"ts": time.time(),
                                       "channel": channel_idx, "text": text})
            mt_ch = self._mt_for_mc(channel_idx)
            if mt_ch is None:
                return  # kanal neni bridgovany (ruzne kanaly existuji soucasne)
            # ochrana proti zacykleni
            if text.startswith(self.cfg.prefix_to_mt) or \
               text.startswith(self.cfg.prefix_to_mc):
                self.stats["mc_dropped"] += 1
                await self._elog("dropped", side="mc", reason="loop-guard",
                                 text=text[:120])
                return
            key = ("mc:" + hashlib.md5(
                f"{channel_idx}{ts}{text}".encode()).hexdigest())
            if self._dedup(key):
                await self._elog("dropped", side="mc", reason="dedup",
                                 text=text[:120])
                return

            log.info("MC->MT: %r", text)
            await self._elog("recv", side="mc", text=text[:200])
            body_limit = self.cfg.mt_max_len - len(self.cfg.prefix_to_mt) - 1
            for chunk in self._chunks(text, body_limit):
                ok = await self._mt_send(f"{self.cfg.prefix_to_mt} {chunk}",
                                         mt_ch)
                if ok:
                    self.stats["mc_to_mt"] += 1
                    await self._elog("forward", direction="mc_to_mt",
                                     text=f"{self.cfg.prefix_to_mt} {chunk}"[:200])
        except Exception:
            log.exception("MC: chyba pri zpracovani zpravy")

    async def _elog(self, event, **kw):
        if self.events:
            try:
                await self.events.log(event, **kw)
            except Exception:
                pass
        if self.db:
            # mirror do SQLite: direction/side do jednoho sloupce, zbytek
            # (snr, mc_tx, pocty udrzby...) jako JSON extra
            side = kw.get("direction") or kw.get("side")
            extra = {k: v for k, v in kw.items()
                     if k not in ("direction", "side", "src", "text")}
            self.db.log(event, direction=side, src=kw.get("src"),
                        text=kw.get("text"), extra=extra or None)

    # ------------------------------------------------------------------ udrzba

    async def _maintenance_loop(self):
        # prvni prubeh chvili po startu (at se vse stihne pripojit)
        await asyncio.sleep(60)
        while self._running:
            try:
                await self._maintain()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("UDRZBA: chyba")
            await asyncio.sleep(self.cfg.maintenance_interval_hours * 3600)

    async def _maintain(self):
        log.info("UDRZBA: zacatek")
        # hodiny na obou zarizenich (zpravy nesou timestampy)
        mc_drift = None
        if self.mc and self.mc.connected:
            mc_drift = await self.mc.sync_clock()
        await self._mt_sync_clock()
        mc_removed = await self._maintain_meshcore()
        mt_removed = await self._maintain_meshtastic()
        log_pruned = self._prune_log()
        await self._elog("maintenance", mc_clock_drift=mc_drift,
                         mc_contacts_removed=mc_removed,
                         mt_nodes_removed=mt_removed,
                         log_pruned=log_pruned)
        self._last_maintenance_ok = time.time()   # health self-check
        self.write_status()
        log.info("UDRZBA: konec")

    async def _mt_sync_clock(self):
        if not self.mt:
            return
        local = self.mt.localNode
        try:
            await asyncio.wait_for(
                self.loop.run_in_executor(None, local.setTime), MT_OP_TIMEOUT)
            log.info("MT: hodiny zarizeni synchronizovany na systemovy cas")
        except asyncio.TimeoutError:
            self._mt_broken(f"setTime timeout (>{MT_OP_TIMEOUT} s)")
        except Exception as e:
            log.warning("MT: nastaveni hodin selhalo: %r", e)

    def _mt_nodeinfo_mqtt(self, packet, decoded):
        """POSITION/TELEMETRY/NODEINFO paket -> <prefix>/mt/nodes/<id>.

        Data se jen preprijou (zadne sifrovani ani bridge logika); topic
        schema je stabilni, entity v HA si uzivatel vytvori dle chuti.
        """
        try:
            nid = packet.get("fromId") or ""
            if not nid or nid == self.my_id:
                return
            payload = {"ts": time.time(), "src": nid,
                       "port": decoded.get("portnum")}
            for key in ("position", "telemetry", "user"):
                if isinstance(decoded.get(key), dict):
                    payload[key] = decoded[key]
            if len(payload) <= 3:
                return
            if packet.get("rxRssi") is not None:
                payload["rssi"] = packet.get("rxRssi")
            if packet.get("rxSnr") is not None:
                payload["snr"] = packet.get("rxSnr")
            self._mqtt_pub(f"mt/nodes/{nid.lstrip('!')}", payload)
        except Exception:
            log.exception("MT: chyba pri publikovani node-info")

    def _publish_mc_self(self):
        """SELF_INFO MeshCore uzlu -> <prefix>/mc/self (bez pub_key)."""
        if not (self.mc and self.mc.info):
            return
        info = {k: v for k, v in self.mc.info.items() if k != "pub_key"}
        self._mqtt_pub("mc/self", dict(info, ts=time.time()))

    async def _forward_mc_position(self):
        """Opt-in: pozice MeshCore uzlu -> Meshtastic sendPosition.

        Posila se jen pri zmene pozice, nebo jednou za 6 h (pozice
        nezmenena); pozice 0/0 (nenastavena) se preskakuje.
        """
        if not (self.cfg.forward_position_mc_to_mt and self.mt and self.mc):
            return
        info = self.mc.info or {}
        lat, lon = info.get("lat", 0.0), info.get("lon", 0.0)
        if not lat or not lon:
            return
        now = time.monotonic()
        if (lat, lon) == getattr(self, "_fwd_pos_last", None) \
                and now - getattr(self, "_fwd_pos_ts", 0.0) < 6 * 3600:
            return
        try:
            await asyncio.wait_for(self.loop.run_in_executor(
                None, lambda: self.mt.sendPosition(lat, lon)), 15)
            self._fwd_pos_last, self._fwd_pos_ts = (lat, lon), now
            log.info("FORWARD: MC pozice %.5f,%.5f odeslana jako "
                     "Meshtastic position paket", lat, lon)
        except Exception as e:
            log.warning("FORWARD: sendPosition selhal: %r", e)

    def _prune_log(self):
        """Smaze zaznamy ve feedu/statistikach starsi nez log_retention_days.

        (0 = ponechat navzdy; spousti se jednou denne s udruzbou.)
        """
        days = self.cfg.log_retention_days
        if days <= 0 or not self.db:
            return 0
        cutoff = time.time() - days * 86400
        try:
            cur = self.db.conn.execute("DELETE FROM log WHERE ts < ?",
                                       (cutoff,))
            self.db.conn.commit()
        except sqlite3.Error as e:
            log.warning("UDRZBA: prune logu selhal: %r", e)
            return 0
        if cur.rowcount:
            log.info("UDRZBA: promazano %d zaznamu starsich %d dnu",
                     cur.rowcount, days)
        return cur.rowcount

    async def _maintain_meshcore(self):
        """Promaze dlouho neaktivni kontakty (favoriti se preskakuji).

        CMD_REMOVE_CONTACT maze i data kontaktu (vc. ulozenych zprav).
        Vraci pocet smazanych.
        """
        if not (self.mc and self.mc.connected):
            log.info("UDRZBA MC: neni pripojeno, preskakuji")
            return 0
        try:
            contacts = await asyncio.wait_for(self.mc.get_contacts(), 60)
        except asyncio.TimeoutError:
            log.warning("UDRZBA MC: timeout pri GET_CONTACTS")
            return 0
        if contacts is None:
            log.warning("UDRZBA MC: GET_CONTACTS selhal")
            return 0
        cutoff = time.time() - self.cfg.mc_contact_max_age_days * 86400
        stale = [c for c in contacts
                 if not c["is_favorite"] and c["lastmod"] < cutoff]
        log.info("UDRZBA MC: %d kontaktu, %d starych (> %d d), limit mazani %d",
                 len(contacts), len(stale), self.cfg.mc_contact_max_age_days,
                 self.cfg.max_removals_per_run)
        removed = 0
        for c in stale[:self.cfg.max_removals_per_run]:
            name = c["name"] or c["pub_key"][:4].hex()
            age_d = (time.time() - c["lastmod"]) / 86400
            if self.cfg.dry_run:
                log.info("UDRZBA MC [DRY-RUN]: smazal bych %s (%.0f dny)",
                         name, age_d)
                continue
            ok = await self.mc.remove_contact(c["pub_key"])
            if ok:
                removed += 1
                self.stats["mc_contacts_removed"] += 1
                log.info("UDRZBA MC: smazan kontakt %s (neaktivni %.0f dny)",
                         name, age_d)
            else:
                log.warning("UDRZBA MC: smazani %s selhalo", name)
        return removed

    async def _maintain_meshtastic(self):
        """Promaze stare uzly z nodeDB (favoriti a my sami se preskakuji).

        Vraci pocet smazanych.
        """
        iface = self.mt
        if not (iface and self.my_id):
            log.info("UDRZBA MT: neni pripojeno, preskakuji")
            return 0
        try:
            cutoff = time.time() - self.cfg.mt_node_max_age_days * 86400
            nodes = dict(iface.nodes or {})
            stale = []
            for nid, nd in nodes.items():
                if nid == self.my_id:
                    continue
                if nd.get("isFavorite") or nd.get("is_favorite"):
                    continue
                if (nd.get("lastHeard") or 0) < cutoff:
                    stale.append((nid, nd))
            log.info("UDRZBA MT: nodeDB %d uzlu, %d starych (> %d d), "
                      "limit mazani %d", len(nodes), len(stale),
                      self.cfg.mt_node_max_age_days,
                      self.cfg.max_removals_per_run)
            if not stale:
                return 0
            local = iface.localNode
            removed = 0

            for nid, nd in stale[:self.cfg.max_removals_per_run]:
                name = (nd.get("user") or {}).get("longName") or nid
                age_d = (time.time() - (nd.get("lastHeard") or 0)) / 86400
                if self.cfg.dry_run:
                    log.info("UDRZBA MT [DRY-RUN]: smazal bych %s (%.0f dny)",
                             name, age_d)
                    continue
                try:
                    await asyncio.wait_for(
                        self.loop.run_in_executor(
                            None, lambda nid=nid: local.removeNode(nid)), 15)
                    removed += 1
                    self.stats["mt_nodes_removed"] += 1
                    log.info("UDRZBA MT: smazan uzel %s (neaktivni %.0f dny)",
                             name, age_d)
                    await asyncio.sleep(2)  # nedrzovat radio admin zpravama
                except asyncio.TimeoutError:
                    log.warning("UDRZBA MT: removeNode %s timeout "
                                "(zavisle spojeni?)", nid)
                except Exception as e:
                    log.warning("UDRZBA MT: smazani %s selhalo: %r", nid, e)
            return removed
        except Exception as e:
            log.warning("UDRZBA MT selhala: %r", e)
            return 0

    # ------------------------------------------------------------------ pomocne

    def _mt_for_mc(self, mc_idx):
        """Mapovany MT kanal pro MC kanal (None = nebridguje se)."""
        for mc, mt in self.mappings:
            if mc == mc_idx:
                return mt
        return None

    def _mc_for_mt(self, mt_idx):
        """Mapovany MC kanal pro MT kanal (None = nebridguje se)."""
        for mc, mt in self.mappings:
            if mt == mt_idx:
                return mc
        return None

    def _db_config(self):
        """(rows, mappings, updated) ze stats.db; prazdne, kdyz DB nema tabulku.

        rows = {'klic': 'hodnota'} (vsechny klice vratne jako str),
        mappings = naparsovane [[mc,mt]] / None (klic chybi),
        updated = timestamp 'config.updated' (None = chyba cteni).
        """
        if not self.db:
            return {}, None, None
        try:
            rows = {r[0]: r[1] for r in
                    self.db.conn.execute("SELECT key, value FROM config")}
        except sqlite3.Error:
            return {}, None, None
        mappings = None
        if rows.get("channels.mappings"):
            try:
                mappings = [(int(a), int(b))
                             for a, b in json.loads(rows["channels.mappings"])]
            except (ValueError, TypeError, json.JSONDecodeError):
                log.warning("Config: neplatne channels.mappings v DB: %r",
                            rows["channels.mappings"])
                mappings = None
        try:
            updated = float(rows.get("config.updated") or 0)
        except (TypeError, ValueError):
            updated = None
        return rows, mappings, updated

    def _db_set_config(self, key, value):
        """Zapise klic do DB config a zvedne config.updated (trigger reloadu)."""
        if not self.db:
            return
        try:
            self.db.conn.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, str(value)))
            self.db.conn.execute(
                "INSERT OR REPLACE INTO config (key, value)"
                " VALUES ('config.updated', ?)", (str(time.time()),))
            self.db.conn.commit()
        except sqlite3.Error as e:
            log.warning("Config: zapis do DB selhal: %r", e)

    def _migrate_mappings_json(self):
        """Jednorazovy import legacy mappings.json do SQLite; [pary]/None."""
        try:
            with open(self.mappings_path, encoding="utf-8") as f:
                pairs = json.load(f).get("mappings")
            pairs = [(int(a), int(b)) for a, b in pairs or []]
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None
        if pairs:
            log.info("Config: migrace mappings.json -> SQLite: %s",
                     ", ".join(f"MC{a}<->MT{b}" for a, b in pairs))
            self._db_set_config("channels.mappings", json.dumps(pairs))
        return pairs

    def _check_config(self):
        """Reload konfigurace/mapovani z DB, kdyz je dashboard prepsal.

        Volano z flush smycky (do 15 s od ulozeni na Config strance).
        """
        rows, mappings, updated = self._db_config()
        if updated is None or updated == self._config_updated:
            return
        self._config_updated = updated
        changed = self.cfg._apply_overlay(rows)
        # hned aplikovatelne vedlejsi efekty
        if "bridge.log_level" in changed:
            logging.getLogger().setLevel(self.cfg.log_level)
        if "meshcore.poll_interval" in changed and self.mc:
            self.mc.poll_interval = self.cfg.mc_poll_interval
        if mappings is not None and mappings != self.mappings:
            self.mappings = mappings
            log.info("Mapovani kanalu: %s",
                     ", ".join(f"MC{a}<->MT{b}" for a, b in mappings)
                     or "(zadne -- nic se nepreposila)")
        if changed:
            log.info("Config reload: %s", ", ".join(changed))
            restart = [k for k in changed if CONFIG_SCHEMA[k].get("restart")]
            if restart:
                log.warning("Config: az po restartu sluzby se projevi: %s",
                            ", ".join(restart))
        # zmena MQTT parametru -> reconnect klienta hned
        if any(k.startswith("mqtt.") for k in changed):
            if self.mqtt:
                self.mqtt.restart()
        self.write_status()

    def _mt_channels_sync(self):
        """Jmena kanalu z localNode.channels (blokuje; volat v executoru).

        Preskakuji se DISABLED sloty bez jmena. Vraci [{'idx','name'}].
        """
        out = []
        try:
            for i, ch in enumerate(self.mt.localNode.channels or []):
                role = int(getattr(ch, "role", 1))  # 0 = DISABLED
                name = (getattr(ch.settings, "name", "")
                        if ch.HasField("settings") else "") or ""
                if role == 0 and not name:
                    continue
                out.append({"idx": i, "name": name})
        except Exception as e:
            log.debug("MT: cteni kanalu selhalo: %r", e)
        return out

    async def _discover_channels(self):
        """Zjisti jmena kanalu obou zarizeni a zapise channels.json (UI)."""
        mc_channels = []
        if self.mc and self.mc.connected:
            try:
                mc_channels = await asyncio.wait_for(self.mc.get_channels(),
                                                      60)
            except asyncio.TimeoutError:
                log.warning("MC: timeout pri zjistovani kanalu")
        mt_channels = []
        if self.mt:
            try:
                mt_channels = await asyncio.wait_for(
                    self.loop.run_in_executor(None, self._mt_channels_sync),
                    MT_OP_TIMEOUT)
            except asyncio.TimeoutError:
                self._mt_broken("cteni kanalu timeout")
        if not (mc_channels or mt_channels):
            return
        try:
            tmp = self.channels_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"updated": int(time.time()),
                           "mc": mc_channels, "mt": mt_channels},
                          f, ensure_ascii=False)
            os.replace(tmp, self.channels_path)
            log.info("Kanaly: MC [%s] | MT [%s]",
                     ",".join(f"{c['idx']}:{c['name']}" for c in mc_channels),
                     ",".join(f"{c['idx']}:{c['name']}" for c in mt_channels))
        except OSError as e:
            log.debug("channels.json write failed: %r", e)

    def _dedup(self, key):
        """True, pokud uz klic byl nedavno viden; jinak si ho zapamatuje."""
        now = time.monotonic()
        if key in self.seen:
            return True
        self.seen[key] = now
        while len(self.seen) > self.cfg.dedup_cache:
            self.seen.popitem(last=False)
        return False

    @staticmethod
    def _chunks(text, limit):
        """Rozseka text na kusy <= limit znaku; delsi zpravy dostanou [i/n]."""
        if limit <= 8:
            limit = 8
        if len(text) <= limit:
            return [text]
        parts = [text[i:i + limit - 8] for i in range(0, len(text), limit - 8)]
        n = len(parts)
        return [f"[{i + 1}/{n}] {p}" for i, p in enumerate(parts)]

    async def _watchdog_loop(self):
        """Ping systemd watchdogu + health self-check.

        WATCHDOG_USEC nastavi systemd (unita Type=notify + WatchdogSec).
        Prestaneme-li pingovat, systemd bridge zabije a diky Restart=always
        ho odstartuje znovu. Health kriterium: denni udruzba musi dobehnout
        do maintenance_interval + grace; pokud ne, bridge je "unhealthy".
        """
        usec = os.getenv("WATCHDOG_USEC")
        interval = min(int(usec) / 2e6, 60.0) if usec else 60.0
        limit = (self.cfg.maintenance_interval_hours * 3600
                 + self.cfg.health_grace_minutes * 60)
        while self._running:
            await asyncio.sleep(interval)
            overdue = time.time() - self._last_maintenance_ok - limit
            if overdue > 0 and self._watchdog_healthy:
                self._watchdog_healthy = False
                log.critical("HEALTH: udruzba nedobehla o %.0f min pres "
                             "limit (%d h + %d min) -- prestavam pingovat "
                             "watchdog, systemd restartuje bridge",
                             overdue / 60, self.cfg.maintenance_interval_hours,
                             self.cfg.health_grace_minutes)
                await self._elog("health", ok=False,
                                 reason="maintenance overdue",
                                 overdue_min=int(overdue / 60))
                self.write_status()
                continue
            if self._watchdog_healthy:
                sd_notify("WATCHDOG=1")

    async def _status_loop(self):
        while self._running:
            await asyncio.sleep(600)
            log.info("STAV: MC=%s MT=%s | prelozeno MC->MT=%d MT->MC=%d, "
                     "filtrovano=%d, dedup=%d | fronty: MC=%d MT=%d | "
                     "udrzba: MC kontakty -%d, MT uzly -%d (posledni OK "
                     "pred %.1f h), repeat-echa=%d | health=%s",
                     "OK" if (self.mc and self.mc.connected) else "NEPRIPOJENO",
                     "OK" if self.mt else "NEPRIPOJENO",
                     self.stats["mc_to_mt"], self.stats["mt_to_mc"],
                     self.stats["mc_dropped"] + self.stats["mt_dropped"],
                     len(self.seen),
                     len(self.mc_pending), len(self.mt_pending),
                     self.stats["mc_contacts_removed"],
                     self.stats["mt_nodes_removed"],
                     (time.time() - self._last_maintenance_ok) / 3600,
                     self.stats["mc_repeated"],
                     "OK" if self._watchdog_healthy else "SELHAL")


# ------------------------------------------------------------------------ main

def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else \
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
    cfg = Config(cfg_path)
    setup_logging(cfg)

    if BLEInterface is None:
        log.error("Chybi knihovna meshtastic (pip install meshtastic).")
        sys.exit(1)

    log.info("=== MeshCore <-> Meshtastic bridge start ===")
    log.info("Konfigurace: MC device=%s | MT device=%s | prefixy '%s' / '%s'",
             cfg.mc_device or "(auto)", cfg.mt_device or "(auto)",
             cfg.prefix_to_mt, cfg.prefix_to_mc)

    bridge = Bridge(cfg)
    log.info("Mapovani kanalu: %s",
             ", ".join(f"MC{mc}<->MT{mt}" for mc, mt in bridge.mappings)
             or "(zadne -- nic se nepreposila)")

    async def runner():
        stop_evt = asyncio.Event()
        loop = asyncio.get_running_loop()
        for s in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(s, stop_evt.set)
            except NotImplementedError:
                signal.signal(s, lambda *a: stop_evt.set())
        bridge_task = asyncio.ensure_future(bridge.run())
        stop_task = asyncio.ensure_future(stop_evt.wait())
        done, _ = await asyncio.wait({bridge_task, stop_task},
                                     return_when=asyncio.FIRST_COMPLETED)
        if stop_task in done:
            bridge_task.cancel()
            try:
                await bridge_task
            except (asyncio.CancelledError, Exception):
                pass
        else:
            bridge_task.result()

    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()