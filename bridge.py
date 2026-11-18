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
import sys
import time
from collections import OrderedDict

from pubsub import pub

from meshcore_ble import MeshCoreClient

try:
    from meshtastic.ble_interface import BLEInterface
except ImportError:  # nezname BLE zarizeni az v runtime, ale knihovna chyti hned
    BLEInterface = None

log = logging.getLogger("bridge")

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
        self.dedup_cache = br.getint("dedup_cache", 400)
        self.log_level = br.get("log_level", "INFO").upper()
        self.log_file = br.get("log_file", "")  # prazdne = jen konzola/journal
        # dashboard: JSONL event log + status soubor (relativni = vedle configu)
        self.event_log = br.get("event_log", "events.jsonl")
        self.status_file = br.get("status_file", "status.json")
        # dashboard: runtime-editovatelne mapovani kanalu + cache jmen kanalu
        self.mappings_file = br.get("mappings_file", "mappings.json")
        self.channels_file = br.get("channels_file", "channels.json")
        self.base_dir = os.path.dirname(os.path.abspath(path))

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
        self.dry_run = (
            ini["maintenance"].getboolean("dry_run", False)
            if ini.has_section("maintenance") else False)

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
        st_path = cfg.status_file
        if st_path and not os.path.isabs(st_path):
            st_path = os.path.join(cfg.base_dir, st_path)
        self.status_path = st_path
        self._last_mc_state = None

        # mapovani kanalu: vychozi z configu; mappings.json (rizene
        # dashboardem) ma prednost a nacita se i za behu (watch mtime)
        self.mappings = list(cfg.mappings)
        self.mappings_path = os.path.join(cfg.base_dir, cfg.mappings_file)
        self._mappings_mtime = -1.0
        self._load_mappings_file()
        self.channels_path = os.path.join(cfg.base_dir, cfg.channels_file)
        self._channels_done = False  # po pripojeni stran zjistime jmena kanalu

        self.mc = None            # MeshCoreClient
        self.mt = None            # meshtastic BLEInterface
        self.my_id = None         # "!xxxxxxxx" naseho T-Beamu

        self.mt_lost = asyncio.Event()
        self.mt_send_lock = asyncio.Lock()
        self.seen = OrderedDict()  # dedup LRU: key -> monotonicky cas
        self.mt_pending = []       # MC->MT zpravy cekajici na pripojeni MT
        self.mc_pending = []       # MT->MC zpravy cekajici na pripojeni MC
        self.stats = {"mc_to_mt": 0, "mt_to_mc": 0,
                      "mc_contacts_removed": 0, "mt_nodes_removed": 0,
                      "mc_dropped": 0, "mt_dropped": 0,
                      "queued": 0, "flushed": 0}

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
            app_name="mesh-bridge")

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
        ]
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
                ok = await self.mc.send_channel_message(mc_ch, msg)
                if ok:
                    self.stats["mt_to_mc"] += 1
                    await self._elog("forward", direction="mt_to_mc",
                                     text=msg[:200])
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
                self._check_mappings()
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
                    ok = await self.mc.send_channel_message(ch, text)
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
                "mt_node": self.my_id,
                "mappings": [list(p) for p in self.mappings],
                "stats": self.stats,
                "queue_mc": len(self.mc_pending),
                "queue_mt": len(self.mt_pending),
            }
            tmp = self.status_path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(st, f, ensure_ascii=False)
            os.replace(tmp, self.status_path)
        except OSError as e:
            log.debug("status write failed: %r", e)

    # --------------------------------------------------------------- meshcore

    async def _on_mc_message(self, text, channel_idx, ts):
        try:
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
        await self._elog("maintenance", mc_clock_drift=mc_drift,
                         mc_contacts_removed=mc_removed,
                         mt_nodes_removed=mt_removed)
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

    def _load_mappings_file(self):
        """Nacte mapovani z mappings.json (jeho zmenou ho ridi dashboard)."""
        try:
            with open(self.mappings_path, encoding="utf-8") as f:
                data = json.load(f)
            pairs = [(int(mc), int(mt)) for mc, mt in data.get("mappings", [])]
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as e:
            log.debug("mappings.json: nelze nacist (%r)", e)
            return
        if pairs != self.mappings:
            log.info("Mapovani kanalu: %s",
                     ", ".join(f"MC{mc}->MT{mt}" for mc, mt in pairs) or "zadne")
        self.mappings = pairs
        try:
            self._mappings_mtime = os.path.getmtime(self.mappings_path)
        except OSError:
            pass

    def _check_mappings(self):
        """Reloadne mappings.json, kdyz ho dashboard prepise (watch mtime)."""
        try:
            mtime = os.path.getmtime(self.mappings_path)
        except OSError:
            return
        if mtime != self._mappings_mtime:
            self._load_mappings_file()
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

    async def _status_loop(self):
        while self._running:
            await asyncio.sleep(600)
            log.info("STAV: MC=%s MT=%s | prelozeno MC->MT=%d MT->MC=%d, "
                     "filtrovano=%d, dedup=%d | fronty: MC=%d MT=%d | "
                     "udrzba: MC kontakty -%d, MT uzly -%d",
                     "OK" if (self.mc and self.mc.connected) else "NEPRIPOJENO",
                     "OK" if self.mt else "NEPRIPOJENO",
                     self.stats["mc_to_mt"], self.stats["mt_to_mc"],
                     self.stats["mc_dropped"] + self.stats["mt_dropped"],
                     len(self.seen),
                     len(self.mc_pending), len(self.mt_pending),
                     self.stats["mc_contacts_removed"],
                     self.stats["mt_nodes_removed"])


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