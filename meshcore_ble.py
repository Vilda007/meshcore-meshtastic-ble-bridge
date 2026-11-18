#!/usr/bin/env python3
"""MeshCore companion-protocol BLE client (strana bridge).

Implementuje podmnožinu MeshCore companion protokolu (dle
MeshCore/docs/companion_protocol.md a examples/companion_radio/MyMesh.cpp):

  - pripojeni ke companion BLE sluzbe (scan podle Service UUID + volitelny
    filtr jmena), APP_START, DEVICE_QUERY, SET_DEVICE_TIME
  - prijem zprav: PUSH_CODE_MSG_WAITING (0x83) + CMD_SYNC_NEXT_MESSAGE (0x0A)
    + periodicky poll (drzi BLE linku ziva a vyzvedne co se minulo)
  - odesilani kanalovych zprav: CMD_SEND_CHANNEL_TXT_MSG (0x03)
    + predikce vysilaneho payloadu (AES-128-ECB je deterministicky ->
      repeat-echo nasi zpravy lze spoznat v raw RX snifferu 0x88)
  - statistiky radia: CMD_GET_STATS / STATS_TYPE_PACKETS (v8+)
  - udrzba: CMD_GET_CONTACTS (0x04) -> CMD_REMOVE_CONTACT (0x0F)
  - auto-reconnect s exponencialnim backoffem

Zasady protokolu: jeden ramec = jedno BLE notification/write; odpoved na
prikaz se rozpoznava podle typu ramce; jeden prikaz v dany okamzik (zamek).
"""

import asyncio
import hashlib
import logging
import struct
import time

from bleak import BleakClient, BleakScanner

log = logging.getLogger("mc")

# BLE UUIDs (companion_protocol.md)
SERVICE = "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
RX_CHAR = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"  # app -> firmware
TX_CHAR = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"  # firmware -> app

# prikazy (MyMesh.cpp)
CMD_APP_START = 0x01
CMD_SEND_CHANNEL_TXT_MSG = 0x03
CMD_GET_CONTACTS = 0x04
CMD_GET_DEVICE_TIME = 0x05
CMD_SET_DEVICE_TIME = 0x06
CMD_SYNC_NEXT_MESSAGE = 0x0A
CMD_REMOVE_CONTACT = 0x0F
CMD_GET_CHANNEL = 0x1F
CMD_SET_CHANNEL = 0x20
CMD_DEVICE_QUERY = 0x16
CMD_GET_STATS = 56          # v8+; druhy bajt = sub-type statistiky
STATS_TYPE_PACKETS = 2

# typ payloadu v raw packetu (packet_format.md)
PAYLOAD_TYPE_GRP_TXT = 0x05

# typy ramcu (companion_protocol.md)
PKT_OK = 0x00
PKT_ERROR = 0x01
PKT_CONTACTS_START = 0x02
PKT_CONTACT = 0x03
PKT_END_OF_CONTACTS = 0x04
PKT_SELF_INFO = 0x05
PKT_MSG_SENT = 0x06
PKT_CONTACT_MSG_RECV = 0x07
PKT_CHANNEL_MSG_RECV = 0x08
PKT_CURR_TIME = 0x09
PKT_NO_MORE_MSGS = 0x0A
PKT_BATTERY = 0x0C
PKT_DEVICE_INFO = 0x0D
PKT_CONTACT_MSG_RECV_V3 = 0x10
PKT_CHANNEL_MSG_RECV_V3 = 0x11
PKT_CHANNEL_INFO = 0x12
PKT_STATS = 24              # RESP_CODE_STATS (0x18; viz stats_binary_frames.md)
PKT_ADVERTISEMENT = 0x80
PKT_ACK = 0x82
PKT_MSG_WAITING = 0x83
PKT_LOG_DATA = 0x88

# odpovedi na CMD_SYNC_NEXT_MESSAGE
GET_MSG_RESPONSES = {PKT_CHANNEL_MSG_RECV, PKT_CHANNEL_MSG_RECV_V3,
                     PKT_CONTACT_MSG_RECV, PKT_CONTACT_MSG_RECV_V3,
                     PKT_NO_MORE_MSGS}


def parse_self_info(frame):
    """PACKET_SELF_INFO (0x05) -> dict (jmeno uzlu, radio, klic)."""
    b = frame
    if len(b) < 58 or b[0] != PKT_SELF_INFO:
        return None
    info = {
        "advert_type": b[1],
        "tx_power_dbm": b[2],
        "max_tx_power": b[3],
        "pub_key": b[4:36].hex(),
        "multi_acks": b[44],
        "telemetry_mode": b[46],
    }
    lat, lon = struct.unpack("<ii", b[36:44])
    info["lat"] = lat / 1e6
    info["lon"] = lon / 1e6
    freq, bw = struct.unpack("<II", b[48:56])
    info["freq_hz"] = freq
    info["bw_hz"] = bw
    info["sf"] = b[56]
    info["cr"] = b[57]
    info["name"] = b[58:].split(b"\x00")[0].decode("utf-8", "replace")
    return info


def parse_device_info(frame):
    """PACKET_DEVICE_INFO (0x0D) -> dict."""
    b = frame
    if len(b) < 3 or b[0] != PKT_DEVICE_INFO:
        return None
    info = {"fw_ver": b[1]}
    if b[1] >= 3 and len(b) >= 80:
        info["max_contacts"] = b[2] * 2
        info["max_channels"] = b[3]
        info["ble_pin"] = struct.unpack("<I", b[4:8])[0]
        info["fw_build"] = b[8:20].decode("utf-8", "replace").rstrip("\x00")
        info["model"] = b[20:60].decode("utf-8", "replace").rstrip("\x00")
        info["ver"] = b[60:80].decode("utf-8", "replace").rstrip("\x00")
    return info


def parse_channel_msg(frame):
    """CHANNEL_MSG_RECV (0x08) / _V3 (0x11) -> dict, nebo None.

    V3 ma SNR*4 v bajtu 1 + 2 rezervovane; standard zacina hned kanalem.
    """
    t = frame[0]
    off = 1
    snr = None
    if t == PKT_CHANNEL_MSG_RECV_V3:
        if len(frame) < 12:
            return None
        snr_raw = frame[1]
        snr = (snr_raw if snr_raw < 128 else snr_raw - 256) / 4.0
        off = 4
    elif t == PKT_CHANNEL_MSG_RECV:
        if len(frame) < 8:
            return None
    else:
        return None
    try:
        channel_idx = frame[off]
        ts = struct.unpack("<I", frame[off + 3:off + 7])[0]
        text = frame[off + 7:].decode("utf-8", "replace")
    except Exception:
        return None
    return {"channel_idx": channel_idx, "timestamp": ts, "text": text, "snr": snr}


def parse_contact_frame(frame):
    """RESP_CODE_CONTACT (0x03) -> dict (pubkey, jmeno, lastmod, flags).

    Format (MyMesh.cpp writeContactRespFrame):
      [0]=0x03 [1:33]pubkey [33]type [34]flags [35]out_path_len
      [36:100]out_path(64) [100:132]name(32) [132:136]last_advert_ts
      [136:140]gps_lat [140:144]gps_lon [144:148]lastmod
    """
    if len(frame) < 148 or frame[0] != PKT_CONTACT:
        return None
    return {
        "pub_key": bytes(frame[1:33]),
        "type": frame[33],
        "flags": frame[34],
        "is_favorite": bool(frame[34] & 0x01),
        "name": frame[100:132].split(b"\x00")[0].decode("utf-8", "replace"),
        "last_advert_ts": struct.unpack("<I", frame[132:136])[0],
        "lastmod": struct.unpack("<I", frame[144:148])[0],
    }


def parse_raw_packet(raw):
    """MeshCore raw packet (packet_format.md) -> dict, nebo None.

    Struktura: [header][transport_codes?][path_len][path][payload].
    Vraci {'route','payload_type','payload'}; transport codes a path
    (hash size 1-3 B, hops 0-63) se preskoci.
    """
    if not raw or len(raw) < 3:
        return None
    header = raw[0]
    route = header & 0x03
    off = 1
    if route in (0x00, 0x03):     # TRANSPORT_FLOOD / TRANSPORT_DIRECT
        off += 4
    if len(raw) < off + 1:
        return None
    plen = raw[off]
    off += 1 + (plen & 0x3F) * (((plen >> 6) & 0x03) + 1)
    if off > len(raw):
        return None
    return {"route": route, "payload_type": (header >> 2) & 0x0F,
            "payload": raw[off:]}


def expected_group_payload(channel_secret, node_name, ts, text):
    """Predpoved payloadu, ktery firmware odvysi jako GRP_TXT nasi zpravu.

    BaseChatMesh::sendGroupMessage + Mesh::createGroupDatagram:
      plaintext = ts LE (4 B) + 0x00 + "<node_name>: <text>"
      sifrovani = AES-128-ECB zero-padded (zadny IV -> deterministicky!)
      payload   = SHA256(secret)[0] (1 B) + HMAC MAC (2 B) + ciphertext

    MAC nespocteme (fw klicuje HMAC 32 B nad 16 B secret), proto vracime
    jen (channel_hash, ciphertext) a porovnavame payload[3:].

    Vraci (channel_hash, ciphertext) nebo None (chybi pycryptodome / klic).
    """
    try:
        from Crypto.Cipher import AES   # pycryptodome
    except ImportError:
        log.debug("MC: chybi pycryptodome -- repeat-echo potvrzeni vypnuto")
        return None
    pt = (struct.pack("<I", ts) + b"\x00"
          + f"{node_name}: {text}".encode("utf-8"))
    pad = (-len(pt)) % 16
    ct = AES.new(bytes(channel_secret), AES.MODE_ECB).encrypt(pt + b"\x00" * pad)
    return hashlib.sha256(bytes(channel_secret)).digest()[0], ct


class MeshCoreClient:
    """BLE klient MeshCore zarizeni; spousti se jako asyncio task run()."""

    def __init__(self, on_message, *, name_filter=None, poll_interval=30.0,
                 app_name="bridge", on_raw_rx=None):
        """
        on_message  -- async callback(text, channel_idx, timestamp)
        name_filter -- Podretezec BLE jmena zarizeni (napr. "MyCoreNode");
                       None = prvni MeshCore companion ve skenu.
        on_raw_rx   -- async callback(pkt dict) pro kazdy prijaty raw
                       packet (sniffer PUSH_CODE_LOG_RX_DATA; slouzi
                       k repeat-echo potvrzeni odvyslanych zprav).
        """
        self.on_message = on_message
        self.name_filter = name_filter
        self.poll_interval = poll_interval
        self.app_name = app_name
        self.on_raw_rx = on_raw_rx

        self.info = {}
        self.device_info = {}
        self.connected = False
        self._stats_ok = None    # None=nezjisteno, bool = CMD_GET_STATS podpora

        self._client = None
        self._loop = None
        self._running = False
        self._write_lock = asyncio.Lock()
        self._disconnected_evt = asyncio.Event()
        self._msg_waiting = asyncio.Event()
        self._in_queue = asyncio.Queue()
        self._raw_queue = asyncio.Queue()
        self._expect_types = None
        self._expect_frames = []
        self._expect_evt = asyncio.Event()
        self._tasks = []

    # ------------------------------------------------------------------ run

    async def run(self):
        """Hlavni task: pripojovaci smycka + drain + dispatch. Nevrati se."""
        self._running = True
        self._loop = asyncio.get_running_loop()
        self._tasks = [
            asyncio.create_task(self._drain_loop(), name="mc-drain"),
            asyncio.create_task(self._dispatch_loop(), name="mc-dispatch"),
            asyncio.create_task(self._raw_loop(), name="mc-raw"),
        ]
        backoff = 5
        while self._running:
            try:
                await self._connect()
                backoff = 5
                log.info("MC: pripojeno (%s)", self.info.get("name", "?"))
                self._disconnected_evt.clear()
                await self._disconnected_evt.wait()
                log.warning("MC: spojeni preruseno")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("MC: pripojeni selhalo: %r", e)
            if not self._running:
                break
            log.info("MC: novy pokus za %d s", backoff)
            try:
                await asyncio.sleep(backoff)
            except asyncio.CancelledError:
                raise
            backoff = min(backoff * 2, 60)

    async def stop(self):
        self._running = False
        self._disconnected_evt.set()
        for t in self._tasks:
            t.cancel()
        if self._client and self._client.is_connected:
            try:
                await self._client.disconnect()
            except Exception:
                pass

    # ------------------------------------------------------------ pripojeni

    async def _find_device(self):
        for _ in range(20):  # ~ 6 minut celkem
            dev = await BleakScanner.find_device_by_filter(
                lambda d, ad: SERVICE in (ad.service_uuids or [])
                and (self.name_filter is None
                     or self.name_filter in (d.name or "")),
                timeout=20.0)
            if dev:
                return dev
            log.info("MC: skenuji (hledam %s)...",
                     self.name_filter or "jakykoliv MeshCore")
        raise ConnectionError("MeshCore zarizeni nenalezeno (BLE sken 6 min)")

    async def _connect(self):
        dev = await self._find_device()
        log.info("MC: nasel %s (%s)", dev.name, dev.address)
        self._client = BleakClient(dev, disconnected_callback=self._on_disconnect)
        try:
            await self._client.connect(timeout=45.0)
        except Exception as e:
            log.warning("MC: connect selhal: %r", e)
            await asyncio.sleep(3)
            await self._client.connect(timeout=45.0)
        await self._client.start_notify(TX_CHAR, self._on_notify)
        self.connected = True

        # APP_START -> SELF_INFO
        resp = await self._send_cmd(
            bytes([CMD_APP_START, 0, 0, 0, 0, 0, 0, 0]) + self.app_name.encode(),
            expect={PKT_SELF_INFO}, timeout=10)
        if resp:
            self.info = parse_self_info(resp) or {}

        # DEVICE_QUERY -> DEVICE_INFO (max kanaly/kontakty, fw verze)
        resp = await self._send_cmd(bytes([CMD_DEVICE_QUERY, 0x03]),
                                    expect={PKT_DEVICE_INFO}, timeout=8)
        if resp:
            self.device_info = parse_device_info(resp) or {}

        # zkontrolovat/nastavit hodiny zarizeni (zpravy nesou timestampy)
        await self.sync_clock()

        # vyzvednout co se nahromadilo mimo spojeni
        self._msg_waiting.set()

    def _on_disconnect(self, _client):
        self.connected = False
        if self._loop:
            self._loop.call_soon_threadsafe(self._disconnected_evt.set)

    # --------------------------------------------------------- prijem ramcu

    def _on_notify(self, _sender, data):
        frame = bytes(data)
        if not frame:
            return
        t = frame[0]
        log.debug("MC <- %s", frame.hex())

        if self._expect_types is not None and t in self._expect_types:
            self._expect_frames.append(frame)
            self._expect_evt.set()

        if t == PKT_MSG_WAITING:
            self._msg_waiting.set()
        elif t in (PKT_CHANNEL_MSG_RECV, PKT_CHANNEL_MSG_RECV_V3):
            self._in_queue.put_nowait(frame)
        elif t == PKT_LOG_DATA:
            # raw RX sniffer: [0]=0x88 [1]=SNR*4 [2]=RSSI [3:]=raw packet
            self._raw_queue.put_nowait(frame)
        elif t == PKT_ERROR:
            code = frame[1] if len(frame) > 1 else -1
            log.warning("MC: PACKET_ERROR kod %s", code)
        elif t == PKT_SELF_INFO:
            self.info = parse_self_info(frame) or self.info
        elif t in (PKT_ADVERTISEMENT, PKT_ACK,
                   PKT_CONTACT_MSG_RECV, PKT_CONTACT_MSG_RECV_V3):
            pass  # nezajimaji nas (DM nebridgujeme)

    async def _raw_loop(self):
        """Konzument snifferu: dekoduje raw packet a vola on_raw_rx."""
        while True:
            frame = await self._raw_queue.get()
            try:
                if len(frame) >= 4:
                    snr_raw = frame[1]
                    rssi = frame[2] if frame[2] < 128 else frame[2] - 256
                    pkt = parse_raw_packet(frame[3:])
                    if pkt and self.on_raw_rx:
                        pkt["snr"] = (snr_raw if snr_raw < 128
                                      else snr_raw - 256) / 4.0
                        pkt["rssi"] = rssi
                        await self.on_raw_rx(pkt)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("MC: chyba pri zpracovani raw packetu")

    async def _dispatch_loop(self):
        while True:
            frame = await self._in_queue.get()
            try:
                msg = parse_channel_msg(frame)
                if msg:
                    await self.on_message(msg["text"], msg["channel_idx"],
                                         msg["timestamp"])
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("MC: chyba pri zpracovani zpravy")

    async def _drain_loop(self):
        """Ceka na PUSH_CODE_MSG_WAITING (nebo poll timeout) a vyzveda frontu.

        Poll interval zaroven funguje jako keepalive BLE spojeni.
        """
        while True:
            try:
                await asyncio.wait_for(self._msg_waiting.wait(),
                                       timeout=self.poll_interval)
            except asyncio.TimeoutError:
                pass
            self._msg_waiting.clear()
            if not self.connected:
                await asyncio.sleep(1)
                continue
            while True:
                resp = await self._send_cmd(bytes([CMD_SYNC_NEXT_MESSAGE]),
                                            expect=GET_MSG_RESPONSES, timeout=8)
                if resp is None or resp[0] == PKT_NO_MORE_MSGS:
                    break
                await asyncio.sleep(0.05)

    # -------------------------------------------------------------- prikazy

    async def _send_cmd(self, payload, *, expect, timeout):
        """Odesle prikaz a ceka na odpoved ocekavaneho typu.

        Vrati ramec odpovedi, nebo None (timeout / odpojeno).
        """
        async with self._write_lock:
            if not (self._client and self._client.is_connected and self.connected):
                return None
            self._expect_types = set(expect)
            self._expect_frames = []
            self._expect_evt.clear()
            try:
                await self._client.write_gatt_char(RX_CHAR, payload,
                                                   response=True)
            except Exception as e:
                log.debug("MC: write selhal: %r", e)
                self._expect_types = None
                return None
            try:
                await asyncio.wait_for(self._expect_evt.wait(), timeout=timeout)
            except asyncio.TimeoutError:
                log.warning("MC: timeout na odpoved (cekal typy %s)",
                            [hex(x) for x in expect])
                self._expect_types = None
                return None
            self._expect_types = None
            frame = self._expect_frames.pop(0)
            log.debug("MC -> %s", frame.hex())
            return frame

    async def send_channel_message(self, channel_idx, text):
        """CMD_SEND_CHANNEL_TXT_MSG; True = zarizeni zpravu prijalo k odeslani.

        Pozn.: na uspech odpovida PACKET_OK (0x00), ne PACKET_MSG_SENT (0x06)
        -- ten patri CMD_SEND_TXT_MSG (DM). Viz MyMesh.cpp:1193 writeOKFrame.
        """
        payload = (bytes([CMD_SEND_CHANNEL_TXT_MSG, 0x00, channel_idx])
                   + struct.pack("<I", int(time.time()))
                   + text.encode("utf-8"))
        resp = await self._send_cmd(payload, expect={PKT_OK, PKT_ERROR},
                                    timeout=10)
        if resp is None:
            return False
        if resp[0] == PKT_ERROR:
            return False
        return True

    async def send_channel_message_confirm(self, channel_idx, text, *,
                                           channel_secret=None,
                                           node_name=None):
        """CMD_SEND_CHANNEL_TXT_MSG + podklady pro potvrzeni doruceni.

        Pred odeslanim si odecte packet-statistiky (srovnani po okne
        potvrdi, ze radio skutecne vysilalo). Zname-li klic kanalu a jmeno
        uzlu, spocta take presny ciphertext, ktery firmware odvysi --
        repeat-echo pak lze spoznat bajtove presne v raw RX snifferu.

        Vraci dict {'ok', 'ts', 'channel_hash', 'ct', 'pre'}: ct = None
        znamena "echo potvrzeni nedorazuje" (chybi klic ci pycryptodome),
        pre = packet-stats pred odeslanim (None = neznamy).
        """
        ts = int(time.time())
        pre = None
        if self._stats_supported():
            pre = await self.get_packet_stats()
        payload = (bytes([CMD_SEND_CHANNEL_TXT_MSG, 0x00, channel_idx])
                   + struct.pack("<I", ts) + text.encode("utf-8"))
        resp = await self._send_cmd(payload, expect={PKT_OK, PKT_ERROR},
                                    timeout=10)
        ok = bool(resp) and resp[0] == PKT_OK
        exp = (expected_group_payload(channel_secret, node_name, ts, text)
               if ok and channel_secret and node_name else None)
        return {"ok": ok, "ts": ts,
                "channel_hash": exp[0] if exp else None,
                "ct": exp[1] if exp else None,
                "pre": pre}

    def _stats_supported(self):
        """True/False podle prvniho pokusu CMD_GET_STATS (cache per spojeni)."""
        if self._stats_ok is None:
            return True      # jeste nezjisteno -- zkusime
        return self._stats_ok

    async def get_packet_stats(self):
        """CMD_GET_STATS/STATS_TYPE_PACKETS -> dict, nebo None (starsi fw).

        Citace jsou kumulativni od bootu (viz docs/stats_binary_frames.md):
        recv, sent, flood_tx, direct_tx, flood_rx, direct_rx (+recv_errors).
        Neuspech pri prvnim pokusu vypne dalsi dotazy (fw statistiky nema).
        """
        resp = await self._send_cmd(
            bytes([CMD_GET_STATS, STATS_TYPE_PACKETS]),
            expect={PKT_STATS, PKT_ERROR}, timeout=8)
        if resp is not None and resp[0] == PKT_ERROR:
            self._stats_ok = False   # fw statistiky nema; dale nezkouset
        if resp is None or resp[0] != PKT_STATS or len(resp) < 26:
            return None
        self._stats_ok = True
        out = dict(zip(("recv", "sent", "flood_tx", "direct_tx",
                        "flood_rx", "direct_rx"),
                       struct.unpack("<6I", resp[2:26])))
        if len(resp) >= 30:
            out["recv_errors"] = struct.unpack("<I", resp[26:30])[0]
        return out

    async def get_channel(self, channel_idx):
        """CMD_GET_CHANNEL -> {'idx','name','secret'}; None = prazdny slot/chyba.

        PACKET_CHANNEL_INFO (companion_protocol.md):
          [0]=0x12 [1]=index [2:34] jmeno (32 B, nulou ukoncene) [34:50] secret
        """
        resp = await self._send_cmd(bytes([CMD_GET_CHANNEL, channel_idx]),
                                    expect={PKT_CHANNEL_INFO, PKT_ERROR},
                                    timeout=8)
        if resp is None or resp[0] != PKT_CHANNEL_INFO or len(resp) < 50:
            return None
        return {
            "idx": resp[1],
            "name": resp[2:34].split(b"\x00")[0].decode("utf-8", "replace"),
            "secret": resp[34:50].hex(),
        }

    async def set_channel(self, channel_idx, name, secret):
        """CMD_SET_CHANNEL: vytvori/prepise slot kanalu; True = ulozeno.

        Ramec: [0]=0x20 [1]=index [2:34] jmeno (32 B, nulou doplnene)
        [34:50] secret (16 B). Firmware klic hashuje SHA256(secret) a kanal
        hned uklada (saveChannels); all-zero secret = cteny "null-key"
        kanal -- nikdy nepouzivat (viz companion_protocol.md).
        """
        if len(secret) != 16:
            raise ValueError("secret musi mit 16 bajtu")
        if len(name.encode("utf-8")) > 32:
            raise ValueError("jmeno kanalu max 32 bajtu")
        payload = (bytes([CMD_SET_CHANNEL, channel_idx])
                   + name.encode("utf-8").ljust(32, b"\x00") + secret)
        resp = await self._send_cmd(payload, expect={PKT_OK, PKT_ERROR},
                                    timeout=10)
        return bool(resp) and resp[0] == PKT_OK

    async def get_channels(self):
        """Jmenovane kanaly zarizeni: [{'idx','name'}...] (prazdne sloty ven).

        Pocet slotu zna DEVICE_QUERY (max_channels); neznamy = nic.
        """
        max_ch = (self.device_info or {}).get("max_channels")
        if not max_ch:
            return []
        out = []
        for i in range(max_ch):
            ch = await self.get_channel(i)
            if ch and ch["name"]:
                out.append({"idx": ch["idx"], "name": ch["name"]})
        return out

    async def get_device_time(self):
        """CMD_GET_DEVICE_TIME -> unix timestamp zarizeni (None pri chybe)."""
        resp = await self._send_cmd(bytes([CMD_GET_DEVICE_TIME]),
                                    expect={PKT_CURR_TIME}, timeout=8)
        if resp is None or len(resp) < 5:
            return None
        return struct.unpack("<I", resp[1:5])[0]

    async def sync_clock(self, max_drift=30):
        """Zkontroluje hodiny zarizeni; pri odchylce > max_drift je nastavi.

        Firmware odmita posun casu zpet (ERR_CODE_ILLEGAL_ARG), takze kdyz je
        zarizeni napred, jen se zaloguje varovani. Vrati drift v sekundach
        (None pokud se hodnoty nepodarilo precist).
        """
        dev_time = await self.get_device_time()
        if dev_time is None:
            log.warning("MC: nelze precist hodiny zarizeni")
            return None
        now = int(time.time())
        drift = now - dev_time
        if abs(drift) <= max_drift:
            log.info("MC: hodiny OK (drift %+d s)", drift)
            return drift
        resp = await self._send_cmd(
            bytes([CMD_SET_DEVICE_TIME]) + struct.pack("<I", now),
            expect={PKT_OK, PKT_ERROR}, timeout=8)
        if resp and resp[0] == PKT_OK:
            log.info("MC: hodiny nastaveny (drift byl %+d s)", drift)
        elif drift < 0:
            log.warning("MC: hodiny jsou o %d s NAPRED a firmware odmita "
                        "posun zpet", -drift)
        else:
            log.warning("MC: nastaveni hodin selhalo (drift %+d s)", drift)
        return drift

    async def get_contacts(self):
        """CMD_GET_CONTACTS -> [contact dict]; viz parse_contact_frame."""
        async with self._write_lock:
            if not (self._client and self._client.is_connected and self.connected):
                return None
            self._expect_types = {PKT_CONTACTS_START, PKT_CONTACT,
                                  PKT_END_OF_CONTACTS}
            self._expect_frames = []
            self._expect_evt.clear()
            try:
                await self._client.write_gatt_char(
                    RX_CHAR, bytes([CMD_GET_CONTACTS]), response=True)
            except Exception as e:
                log.warning("MC: GET_CONTACTS write selhal: %r", e)
                self._expect_types = None
                return None
            contacts = []
            idle_deadline = 30.0
            try:
                while True:
                    await asyncio.wait_for(self._expect_evt.wait(),
                                           timeout=idle_deadline)
                    self._expect_evt.clear()
                    frames, self._expect_frames = self._expect_frames, []
                    done = False
                    for f in frames:
                        if f[0] == PKT_CONTACT:
                            c = parse_contact_frame(f)
                            if c:
                                contacts.append(c)
                        elif f[0] == PKT_END_OF_CONTACTS:
                            done = True
                    if done:
                        return contacts
            except asyncio.TimeoutError:
                log.warning("MC: GET_CONTACTS timeout (nedorazil END)")
                return contacts or None
            finally:
                self._expect_types = None

    async def remove_contact(self, pub_key):
        """CMD_REMOVE_CONTACT (32bajtovy pubkey); maze i ulozene zpravy kontaktu."""
        if len(pub_key) != 32:
            raise ValueError("pub_key musi mit 32 bajtu")
        resp = await self._send_cmd(bytes([CMD_REMOVE_CONTACT]) + pub_key,
                                    expect={PKT_OK, PKT_ERROR}, timeout=10)
        if resp is None:
            return False
        return resp[0] == PKT_OK