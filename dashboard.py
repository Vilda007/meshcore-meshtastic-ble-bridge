#!/usr/bin/env python3
"""Web dashboard pro MeshCore <-> Meshtastic bridge (bezi na Raspberry Pi).

Cte status.json / SQLite (stats.db: feed zprav a udalosti, konfigurace);
jazyk EN/CS (cookie 'mclang', vychozi EN).

  http://<pi-ip>:8420/

Auth: prihlaseni uzivatele (tabulka 'users' ve stats.db, PBKDF2 hesla,
session cookie 'mbsid', 30 dni). Prihlaseny admin muze menit konfiguraci
(stranka Config -> tabulka 'config', bridge prebira do 15 s), mapovani
kanalu, spravovat uzivatele a vypinat bridge. Bez prihlaseni jen login;
obycejny uzivatel vidi jen prehled (read-only).
"""

import datetime
import hashlib
import json
import os
import re
import secrets
import sqlite3
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from config_schema import CONFIG_SCHEMA, config_sections

BASE = os.path.dirname(os.path.abspath(__file__))
EVENTS = os.path.join(BASE, "events.jsonl")
STATUS = os.path.join(BASE, "status.json")
CHANNELS = os.path.join(BASE, "channels.json")
DB = os.path.join(BASE, "stats.db")   # SQLite feed + statistiky + config + users
# Volitelny soubor s pristupovymi udaji bridgovaneho kanalu; NEPOSOILET
# do repozitare -- obsahuje klice! (viz channel_info.json.example)
CHANNEL_INFO = os.path.join(BASE, "channel_info.json")
PORT = 8420

BRIDGE_UNIT = "mesh-bridge"
SYSTEMCTL = "/usr/bin/systemctl"
SESSION_COOKIE = "mbsid"
SESSION_MAX_AGE = 30 * 86400


def bridge_running():
    """True, pokud systemd unita bridge bezi ('active')."""
    try:
        out = subprocess.run([SYSTEMCTL, "is-active", BRIDGE_UNIT],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() == "active"
    except (OSError, subprocess.TimeoutExpired):
        return False


def read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


def write_json_atomic(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


# --------------------------------------------------------------------- auth

def hash_pw(pw, salt=None):
    """PBKDF2-SHA256 heslo -> 'pbkdf2$<salt>$<hash>' (stdlib only)."""
    salt = salt or secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(),
                                 200_000).hex()
    return f"pbkdf2${salt}${digest}"


def check_pw(pw, stored):
    try:
        _, salt, digest = stored.split("$")
    except (ValueError, AttributeError):
        return False
    calc = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt.encode(),
                               200_000).hex()
    return secrets.compare_digest(calc, digest)


def init_db():
    """Vytvori tabulky config/users/sessions; prvni beh = default admin."""
    conn = db_connect()
    try:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            pw_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL DEFAULT 0,
            created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires REAL NOT NULL);
        """)
        if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
            conn.execute("INSERT INTO users (username, pw_hash, is_admin,"
                         " created) VALUES (?,?,?,?)",
                         ("admin", hash_pw("admin"), 1, time.time()))
            print("WARNING: created default admin 'admin'/'admin' "
                  "-- CHANGE THE PASSWORD!")
        conn.commit()
    finally:
        conn.close()


# ------------------------------------------------------------------- SQLite

def db_connect():
    conn = sqlite3.connect(DB, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def db_get_config(conn, key):
    row = conn.execute("SELECT value FROM config WHERE key = ?",
                       (key,)).fetchone()
    return row["value"] if row else None


def db_set_config(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                 (key, str(value)))
    conn.execute("INSERT OR REPLACE INTO config (key, value)"
                 " VALUES ('config.updated', ?)", (str(time.time()),))


FEED_FILTERS = {
    "all": None,
    "recv-mc": "kind='recv' AND direction='mc'",
    "recv-mt": "kind='recv' AND direction='mt'",
    "fwd": "kind IN ('forward','flush','mc_repeat','mc_confirm')",
    "drop": "kind='dropped'",
    "state": "kind IN ('connect','disconnect','maintenance','health')",
}


def query_feed(flt, q, offset, limit):
    """Strankovany + prohledavatelny feed z tabulky 'log' (bridge.py)."""
    where, params = [], []
    cond = FEED_FILTERS.get(flt)
    if cond:
        where.append(cond)
    if q:
        where.append("(text LIKE ? OR src LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    conn = db_connect()
    try:
        total = conn.execute("SELECT COUNT(*) FROM log" + wsql,
                             params).fetchone()[0]
        rows = conn.execute(
            "SELECT ts, kind, direction, src, text, extra FROM log" + wsql
            + " ORDER BY ts DESC LIMIT ? OFFSET ?",
            params + [limit, offset]).fetchall()
    except sqlite3.Error:
        return {"rows": [], "total": 0, "offset": offset, "limit": limit}
    finally:
        conn.close()
    out = []
    for r in rows:
        ev = {"ts": r["ts"], "event": r["kind"], "direction": r["direction"],
              "src": r["src"], "text": r["text"]}
        if r["kind"] in ("recv", "connect", "disconnect"):
            ev["side"] = r["direction"]      # u techto druhu je v nem strana
        if r["extra"]:
            try:
                ev.update(json.loads(r["extra"]))
            except (json.JSONDecodeError, TypeError):
                pass
        out.append(ev)
    return {"rows": out, "total": total, "offset": offset, "limit": limit}


def query_history(days=30):
    """Denni rady pro grafy: zpravy, reconnecty, uptime % obou stran."""
    days = max(1, min(days, 365))
    today = datetime.date.today()
    dates = [today - datetime.timedelta(days=i)
             for i in range(days - 1, -1, -1)]
    starts = [datetime.datetime.combine(d, datetime.time.min).timestamp()
              for d in dates]
    since = starts[0]
    now = time.time()
    conn = db_connect()
    try:
        fwd = {}
        for r in conn.execute(
                "SELECT CAST((ts - ?) / 86400 AS INTEGER) AS d, direction,"
                " COUNT(*) AS c FROM log"
                " WHERE kind='forward' AND ts >= ? GROUP BY d, direction",
                (since, since)):
            fwd[(r["d"], r["direction"])] = r["c"]
        recon = dict(conn.execute(
            "SELECT CAST((ts - ?) / 86400 AS INTEGER) AS d, COUNT(*) AS c"
            " FROM log WHERE kind='disconnect' AND ts >= ? GROUP BY d",
            (since, since)).fetchall())
        state = {"mc": False, "mt": False}
        open_since = {"mc": None, "mt": None}
        secs = {d: {"mc": 0.0, "mt": 0.0} for d in range(days)}
        denom = [86400.0] * days
        denom[-1] = max(1.0, now - starts[-1])

        def close(side, t):
            if open_since[side] is not None and t > open_since[side]:
                t0 = max(open_since[side], since)
                t1 = min(t, now)
                d0 = int((t0 - since) // 86400)
                d1 = int((t1 - since) // 86400)
                for d in range(max(0, d0), min(days - 1, d1) + 1):
                    lo = max(t0, starts[d])
                    hi = min(t1, starts[d] + 86400)
                    if hi > lo:
                        secs[d][side] += hi - lo
            open_since[side] = None

        for side in ("mc", "mt"):
            row = conn.execute(
                "SELECT kind FROM log WHERE kind IN ('connect','disconnect')"
                " AND direction=? AND ts < ? ORDER BY ts DESC LIMIT 1",
                (side, since)).fetchone()
            if row and row["kind"] == "connect":
                state[side] = True
                open_since[side] = since
        for r in conn.execute(
                "SELECT ts, kind, direction FROM log WHERE kind IN"
                " ('connect','disconnect','bridge_start','bridge_stop')"
                " AND ts >= ? ORDER BY ts", (since,)):
            k = r["kind"]
            if k == "connect":
                side = r["direction"]
                if side in state and not state[side]:
                    state[side] = True
                    open_since[side] = r["ts"]
            elif k == "disconnect":
                side = r["direction"]
                if side in state and state[side]:
                    state[side] = False
                    close(side, r["ts"])
            elif k in ("bridge_start", "bridge_stop"):
                for side in ("mc", "mt"):
                    close(side, r["ts"])
                    state[side] = False
        for side in ("mc", "mt"):
            if state[side]:
                close(side, now)
    except sqlite3.Error:
        return None
    finally:
        conn.close()
    return {
        "days": [d.isoformat() for d in dates],
        "mc_to_mt": [fwd.get((d, "mc_to_mt"), 0) for d in range(days)],
        "mt_to_mc": [fwd.get((d, "mt_to_mc"), 0) for d in range(days)],
        "reconnects": [recon.get(d, 0) for d in range(days)],
        "mc_up": [round(min(100, secs[d]["mc"] / denom[d] * 100), 1)
                  for d in range(days)],
        "mt_up": [round(min(100, secs[d]["mt"] / denom[d] * 100), 1)
                  for d in range(days)],
    }


def valid_mappings(data):
    """Validace tela POST /api/mappings -> [[mc, mt], ...] nebo None."""
    try:
        pairs = data["mappings"]
        if not isinstance(pairs, list):
            return None
    except (KeyError, TypeError):
        return None
    out, seen_mc, seen_mt = [], set(), set()
    for p in pairs:
        try:
            mc, mt = int(p[0]), int(p[1])
        except (TypeError, ValueError, IndexError):
            return None
        if not (0 <= mc < 64 and 0 <= mt < 8):
            return None
        if mc in seen_mc or mt in seen_mt:
            return None
        out.append([mc, mt])
        seen_mc.add(mc)
        seen_mt.add(mt)
    return out

PAGE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mesh bridge</title>
<style>
:root {
  --bg: #0f1216; --panel: #171c22; --line: #262d36; --fg: #dbe2ea;
  --dim: #8b96a3; --mc: #7ec46e; --mt: #6ea8d8; --warn: #d8a15e;
  --drop: #a07070; --con: #55c58a; --dis: #d06b6b;
}
@media (prefers-color-scheme: light) {
  :root { --bg:#f2f4f6; --panel:#ffffff; --line:#d9dee4; --fg:#20262e;
          --dim:#5f6b77; --mc:#3f8f2f; --mt:#2065a0; --warn:#a06820;
          --drop:#a05050; --con:#22874e; --dis:#c04040; }
}
* { box-sizing: border-box; }
body { margin:0; background:var(--bg); color:var(--fg);
       font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }
.wrap { max-width:900px; margin:0 auto; padding:16px; }
h1 { font-size:18px; margin:0 0 10px; display:flex; gap:10px;
     align-items:center; flex-wrap:wrap; }
h1 .sub { color:var(--dim); font-weight:400; font-size:13px; }
#who { color:var(--dim); font-size:13px; margin-left:auto; }
#powerBtn, #langBtn, #logoutBtn, .viewbtn { display:inline-flex;
  align-items:center; gap:7px; cursor:pointer; font:600 13px/1 inherit;
  padding:7px 14px; border-radius:20px; border:1px solid var(--line);
  background:var(--panel); color:var(--fg); }
#powerBtn.on { border-color:var(--con); color:var(--con); }
#powerBtn.off { border-color:var(--dis); color:var(--dis); }
#powerBtn:disabled { opacity:.5; cursor:wait; }
#powerBtn .pk { width:10px; height:10px; border-radius:50%; }
#powerBtn.on .pk { background:var(--con); }
#powerBtn.off .pk { background:var(--dis); }
#langBtn { padding:7px 10px; }
#logoutBtn { color:var(--dim); }
.views { display:flex; gap:6px; margin-bottom:14px; flex-wrap:wrap; }
.viewbtn.active { border-color:var(--mt); color:var(--mt); }
.cards { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
.card { background:var(--panel); border:1px solid var(--line);
        border-radius:10px; padding:10px 14px; min-width:150px; }
.card .k { color:var(--dim); font-size:11px; text-transform:uppercase;
           letter-spacing:.05em; }
.card .v { font-size:18px; font-weight:600; }
.panel { background:var(--panel); border:1px solid var(--line);
  border-radius:10px; padding:12px 14px; margin-bottom:14px; }
.panel h2 { font-size:14px; margin:0 0 4px; }
.panel h3 { font-size:13px; margin:10px 0 6px; color:var(--dim); }
.panel .hint { color:var(--dim); font-size:12px; margin:0 0 10px; }
.mrow { display:flex; gap:8px; align-items:center; margin-bottom:8px;
  flex-wrap:wrap; }
.mrow .arr { color:var(--dim); font-size:12px; }
.msel, .mnum, .crow input, .crow select, .urow2 input {
  background:var(--bg); color:var(--fg); border:1px solid var(--line);
  border-radius:6px; padding:5px 8px; font-size:13px; max-width:220px; }
.msel.mc { border-color:rgba(126,196,110,.4); }
.msel.mt { border-color:rgba(110,168,216,.4); }
.mnum { width:70px; }
.mrm { background:none; border:none; color:var(--drop); font-size:16px;
  cursor:pointer; padding:2px 8px; }
.mrm:disabled { opacity:.3; cursor:default; }
.mbtn { background:var(--panel); color:var(--fg); border:1px solid var(--line);
  border-radius:20px; padding:5px 14px; cursor:pointer; font-size:12px; }
.mbtn.primary { border-color:var(--mt); color:var(--mt); font-weight:600; }
#mapNote, #cfgNote, #usersNote { font-size:12px; margin-left:10px; }
#mapNote.ok, #cfgNote.ok, #usersNote.ok { color:var(--con); }
#mapNote.warn, #cfgNote.warn, #usersNote.warn { color:var(--warn); }
.dot { display:inline-block; width:9px; height:9px; border-radius:50%;
       margin-right:6px; background:var(--dim); }
.dot.on { background:var(--con); }
.dot.off { background:var(--dis); }
.filters { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:10px; }
.filters button { background:var(--panel); color:var(--fg);
  border:1px solid var(--line); border-radius:20px; padding:4px 12px;
  cursor:pointer; font-size:12px; }
.filters button.active { border-color:var(--mt); color:var(--mt); }
#list { display:flex; flex-direction:column; gap:6px; }
.row { background:var(--panel); border:1px solid var(--line);
       border-radius:8px; padding:8px 12px; display:flex; gap:10px;
       align-items:baseline; }
.row .t { color:var(--dim); font-size:12px; white-space:nowrap;
          font-variant-numeric:tabular-nums; }
.badge { font-size:10px; font-weight:700; padding:2px 7px;
         border-radius:4px; white-space:nowrap; letter-spacing:.03em; }
.badge.recv-mc  { background:rgba(126,196,110,.15); color:var(--mc); }
.badge.recv-mt  { background:rgba(110,168,216,.15); color:var(--mt); }
.badge.fwd-mt   { background:rgba(126,196,110,.22); color:var(--mc); }
.badge.fwd-mc   { background:rgba(110,168,216,.22); color:var(--mt); }
.badge.rep      { background:rgba(85,197,138,.18); color:var(--con); }
.badge.drop     { background:rgba(160,112,112,.15); color:var(--drop); }
.badge.q        { background:rgba(216,161,94,.15); color:var(--warn); }
.badge.st       { background:rgba(139,150,163,.12); color:var(--dim); }
.row .txt { flex:1; overflow-wrap:anywhere; }
.row .src { color:var(--dim); font-size:12px; }
.chrow { display:flex; gap:8px; align-items:baseline; margin-bottom:6px;
  flex-wrap:wrap; }
.chrow:last-child { margin-bottom:0; }
.chrow code { font:12px/1.4 ui-monospace,Consolas,monospace;
  background:var(--bg); border:1px solid var(--line); border-radius:4px;
  padding:2px 6px; overflow-wrap:anywhere; }
.chrow .hex, .crow .hex { color:var(--dim); font-size:12px; }
.tag { font-size:10px; font-weight:700; padding:2px 7px;
  border-radius:4px; white-space:nowrap; }
.tag.mc { background:rgba(126,196,110,.15); color:var(--mc); }
.tag.mt { background:rgba(110,168,216,.15); color:var(--mt); }
a { color:var(--mt); }
.listbar { display:flex; gap:8px; align-items:center; margin-bottom:10px;
  flex-wrap:wrap; }
#search { background:var(--panel); color:var(--fg); border:1px solid var(--line);
  border-radius:20px; padding:5px 12px; font-size:13px; min-width:180px; }
#pager { color:var(--dim); font-size:12px; display:flex; gap:6px;
  align-items:center; }
#pager button { background:var(--panel); color:var(--fg); border:1px solid var(--line);
  border-radius:4px; padding:2px 10px; cursor:pointer; font-size:13px; }
#pager button:disabled { opacity:.4; cursor:default; }
.chart { margin-bottom:12px; }
.chart:last-child { margin-bottom:0; }
.chart h3 { font-size:12px; margin:0 0 4px; color:var(--dim); font-weight:600; }
.bars { display:flex; align-items:flex-end; gap:2px; height:70px; }
.col { flex:1; min-width:6px; height:100%; display:flex; align-items:flex-end;
  justify-content:center; gap:1px; }
.bar { border-radius:2px 2px 0 0; min-height:1px; width:100%; max-width:14px; }
.bar.mc { background:var(--mc); }
.bar.mt { background:var(--mt); }
.bar.warn { background:var(--warn); }
.crow { display:flex; align-items:center; gap:10px; margin-bottom:6px;
  flex-wrap:wrap; }
.crow label { flex:1; min-width:220px; color:var(--dim); font-size:12px; }
.crow input, .crow select { flex:0 1 240px; max-width:240px; }
.urow { display:flex; gap:10px; align-items:center; margin-bottom:6px;
  flex-wrap:wrap; }
.urow2 { display:flex; gap:8px; align-items:center; margin-bottom:6px;
  flex-wrap:wrap; }
.urow2 label { color:var(--dim); font-size:12px; min-width:160px; }
footer { color:var(--dim); font-size:12px; margin-top:16px; }
#loginOverlay { position:fixed; inset:0; background:rgba(10,12,16,.75);
  display:flex; align-items:center; justify-content:center; z-index:50; }
#loginOverlay[hidden] { display:none; }
.loginbox { background:var(--panel); border:1px solid var(--line);
  border-radius:12px; padding:24px 28px; width:min(340px, 90vw);
  display:flex; flex-direction:column; gap:10px; }
.loginbox h2 { margin:0; font-size:16px; }
.loginbox input { background:var(--bg); color:var(--fg);
  border:1px solid var(--line); border-radius:6px; padding:8px 10px;
  font-size:14px; }
.loginbox .warnbox { color:var(--warn); font-size:12px; margin:0; }
#liErr { color:var(--dis); font-size:12px; margin:0; min-height:16px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Mesh bridge <span class="sub">MeshCore &harr; Meshtastic</span>
      <button id="powerBtn" class="off"><span class="pk"></span>
      <span id="powerTxt">…</span></button>
      <button id="langBtn" title="Language / Jazyk">🇬🇧 EN</button>
      <button id="logoutBtn" hidden data-i18n="logout">Log out</button>
      <span id="who"></span></h1>
  <div class="views">
    <button class="viewbtn active" data-v="dash" data-i18n="v_dash">Dashboard</button>
    <button class="viewbtn" data-v="config" data-i18n="v_config">Config</button>
    <button class="viewbtn" data-v="users" id="usersTab" hidden
      data-i18n="v_users">Users</button>
  </div>

  <div id="viewDash">
    <div class="cards" id="cards"></div>
    <div class="panel" id="mapPanel">
      <h2 data-i18n="map_title">Channel mapping</h2>
      <p class="hint" data-i18n="map_hint">Every MeshCore channel is relayed
        to a paired Meshtastic channel and back. The bridge picks changes up
        within ~15 s (or at start). One channel can be in the mapping only
        once. Channel names load automatically once the bridge connects to
        both devices — until then, index numbers are used.</p>
      <div id="mapRows"></div>
      <button class="mbtn" id="mapAdd" data-i18n="map_add">+ add mapping</button>
      <button class="mbtn primary" id="mapSave" data-i18n="map_save">Save</button>
      <span id="mapNote"></span>
    </div>
    <div class="panel" id="channelPanel" hidden></div>
    <div class="panel" id="statsPanel" hidden>
      <h2 data-i18n="stats_title">Statistics</h2>
      <div id="charts"></div>
    </div>
    <div class="filters" id="filters"></div>
    <div class="listbar">
      <input type="search" id="search" placeholder="Search…">
      <span id="pager"></span>
    </div>
    <div id="list"></div>
    <footer><span data-i18n="refresh">Last refresh</span>:
     <span id="refresh"></span>
     &middot; <span data-i18n="footer_pi">bridge on Raspberry Pi</span>
     &middot; <a href="https://github.com/Vilda007/meshcore-meshtastic-ble-bridge"
        target="_blank" rel="noopener">GitHub</a></footer>
  </div>

  <div id="viewConfig" hidden>
    <div class="panel">
      <h2 data-i18n="v_config">Config</h2>
      <p class="hint"><span data-i18n="cfg_hint">Configuration is stored in
        SQLite; the bridge picks changes up within ~15 s. Items marked
        &#8635; apply only after a bridge restart.</span></p>
      <div id="cfgForm"></div>
      <button class="mbtn primary" id="cfgSave" data-i18n="cfg_save">Save</button>
      <span id="cfgNote"></span>
    </div>
  </div>

  <div id="viewUsers" hidden>
    <div class="panel">
      <h2 data-i18n="v_users">Users</h2>
      <div id="usersRows"></div>
      <h3 data-i18n="u_add">Add user</h3>
      <div class="urow2">
        <input id="nuName" placeholder="Username">
        <input id="nuPass" type="password" placeholder="Password">
        <label><input type="checkbox" id="nuAdmin"> admin</label>
        <button class="mbtn primary" id="nuAdd" data-i18n="u_add">Add user</button>
      </div>
    </div>
    <div class="panel">
      <h2 data-i18n="u_pw_title">Change my password</h2>
      <div class="urow2">
        <input id="pwOld" type="password" data-i18n-ph="u_pw_old"
          placeholder="Current password">
        <input id="pwNew" type="password" data-i18n-ph="u_pw_new"
          placeholder="New password">
        <button class="mbtn primary" id="pwSave" data-i18n="map_save">Save</button>
      </div>
      <span id="pwNote" class="hint"></span>
    </div>
  </div>
</div>

<div id="loginOverlay" hidden>
  <div class="loginbox">
    <h2>Mesh bridge</h2>
    <p class="warnbox" id="defPwWarn" hidden data-i18n="default_pw">
      Default admin is admin/admin — change it immediately!</p>
    <input id="liUser" data-i18n-ph="li_user" placeholder="Username">
    <input id="liPass" type="password" data-i18n-ph="li_pass"
      placeholder="Password">
    <button class="mbtn primary" id="liBtn" data-i18n="li_btn">Sign in</button>
    <p id="liErr"></p>
  </div>
</div>

<script>
// pristupove udaje bridgovaneho kanalu (channel_info.json na serveru);
// null = soubor neexistuje, panel se nezobrazi
const CHANNEL_INFO = __CHANNEL_INFO__;

// ------------------------------------------------------------------- i18n
// vychozi jazyk EN, vyber v cookie 'mclang' (prepinalac s vlajeckou)
const I18N = {
  en: {
    sub: "MeshCore ↔ Meshtastic",
    power_on: "running · stop", power_off: "stopped · start",
    power_on_cmd: "starting…", power_off_cmd: "stopping…",
    power_err: "⚠ systemctl failed",
    conn_on: "connected", conn_off: "disconnected", bridge_off: "off",
    card_mc: "MeshCore", card_mt: "Meshtastic",
    card_fwd: "MC▸MT", card_back: "MT▸MC",
    card_queues: "Queues",
    card_maint_mc: "Maintenance MeshCore",
    card_maint_mt: "Maintenance Meshtastic",
    contacts: " contacts", nodes: " nodes removed",
    map_title: "Channel mapping",
    map_hint: "Every MeshCore channel is relayed to a paired Meshtastic channel and back. The bridge picks changes up within ~15 s (or at start). One channel can be in the mapping only once. Channel names load automatically once the bridge connects to both devices — until then, index numbers are used.",
    map_add: "+ add mapping", map_save: "Save",
    map_saved_ok: "Saved ✓ — the bridge picks it up within ~15 s.",
    map_dup: "Each channel can appear in the mapping only once.",
    map_save_fail: "Saving failed.",
    unnamed: "(unnamed)",
    f_all: "All", f_recv_mc: "Received MC", f_recv_mt: "Received MT",
    f_fwd: "Forwarded", f_drop: "Filtered", f_state: "Events",
    b_recv_mc: "MC ▸ receive", b_recv_mt: "MT ▸ receive",
    b_fwd_mt: "MC ▸▸ MT", b_fwd_mc: "MT ▸▸ MC",
    b_drop: "filter: ", b_queued: "queued", b_flush: "flushed from queue",
    b_connect: " connected", b_disconnect: " disconnected",
    b_maint: "maintenance: MC −{mc} contacts, MT −{mt} nodes",
    b_repeat: "repeater echo ✓ SNR {snr} dB · {rssi} dBm · {delay} s",
    b_conf_tx: "on air (TX counter)", b_conf_no: "no air activity",
    b_conf_na: "air check unavailable",
    b_health: "health check FAILED",
    from: "from ",
    search_ph: "Search…",
    stats_title: "Statistics",
    c_msgs: "Forwarded messages / day",
    c_recon: "Reconnects / day",
    c_uptime: "Uptime — connected % per day",
    refresh: "Last refresh", footer_pi: "bridge on Raspberry Pi",
    v_dash: "Dashboard", v_config: "Config", v_users: "Users",
    logout: "Log out",
    li_user: "Username", li_pass: "Password", li_btn: "Sign in",
    li_err: "Wrong username or password.",
    default_pw: "Default admin is admin/admin — change it immediately!",
    cfg_hint: "Configuration is stored in SQLite; the bridge picks changes up within ~15 s. Items marked ↻ apply only after a bridge restart.",
    cfg_save: "Save",
    cfg_saved: "Saved ✓ — the bridge picks it up within ~15 s.",
    cfg_restart: "Needs bridge restart:",
    cfg_save_fail: "Saving failed — check the values.",
    u_add: "Add user",
    u_pw_title: "Change my password",
    u_pw_old: "Current password", u_pw_new: "New password",
    pw_saved: "Password changed ✓.", pw_fail: "Change failed.",
    u_saved: "User added ✓.", u_del: "×",
    generic_fail: "Failed."
  },
  cs: {
    sub: "MeshCore ↔ Meshtastic",
    power_on: "běží · vypnout", power_off: "vypnut · zapnout",
    power_on_cmd: "zapínám…", power_off_cmd: "vypínám…",
    power_err: "⚠ systemctl selhal",
    conn_on: "připojeno", conn_off: "odpojeno", bridge_off: "vypnuto",
    card_mc: "MeshCore", card_mt: "Meshtastic",
    card_fwd: "MC▸MT", card_back: "MT▸MC",
    card_queues: "Fronty",
    card_maint_mc: "Údržba MeshCore",
    card_maint_mt: "Údržba Meshtastic",
    contacts: " kontaktů", nodes: " uzlů smazáno",
    map_title: "Mapování kanálů",
    map_hint: "Každý MeshCore kanál se přeposílá na spárovaný Meshtastic kanál a zpět. Změny bridge přebere do ~15 s (nebo při startu). Jeden kanál může být v mapování jen jednou. Názvy kanálů se načtou samy, až se bridge připojí k oběma zařízením — dokud ne, zadávají se čísla indexů.",
    map_add: "+ přidat mapování", map_save: "Uložit",
    map_saved_ok: "Uloženo ✓ — bridge přebere do ~15 s.",
    map_dup: "Každý kanál může být v mapování jen jednou.",
    map_save_fail: "Uložení selhalo.",
    unnamed: "(bezejmenný)",
    f_all: "Vše", f_recv_mc: "Příjem MC", f_recv_mt: "Příjem MT",
    f_fwd: "Předané", f_drop: "Filtrované", f_state: "Stavy",
    b_recv_mc: "MC ▸ příjem", b_recv_mt: "MT ▸ příjem",
    b_fwd_mt: "MC ▸▸ MT", b_fwd_mc: "MT ▸▸ MC",
    b_drop: "filtr: ", b_queued: "ve frontě", b_flush: "vypláceno z fronty",
    b_connect: " připojeno", b_disconnect: " odpojeno",
    b_maint: "údržba: MC −{mc} kontaktů, MT −{mt} uzlů",
    b_repeat: "echo repeateru ✓ SNR {snr} dB · {rssi} dBm · {delay} s",
    b_conf_tx: "vysíláno (TX čítač)", b_conf_no: "bez vysílání",
    b_conf_na: "kontrola vysílání nedostupná",
    b_health: "health kontrola SELHALA",
    from: "od ",
    search_ph: "Hledat…",
    stats_title: "Statistiky",
    c_msgs: "Přeposlané zprávy / den",
    c_recon: "Reconnecty / den",
    c_uptime: "Uptime — % připojení za den",
    refresh: "Poslední obnovení", footer_pi: "bridge na Raspberry Pi",
    v_dash: "Přehled", v_config: "Nastavení", v_users: "Uživatelé",
    logout: "Odhlásit",
    li_user: "Uživatel", li_pass: "Heslo", li_btn: "Přihlásit",
    li_err: "Chybné uživatel/heslo.",
    default_pw: "Výchozí admin je admin/admin — okamžitě změňte heslo!",
    cfg_hint: "Konfigurace se ukládá do SQLite; bridge přebere změny do ~15 s. Položky označené ↻ se projeví až po restartu bridge.",
    cfg_save: "Uložit",
    cfg_saved: "Uloženo ✓ — bridge přebere do ~15 s.",
    cfg_restart: "Vyžaduje restart bridge:",
    cfg_save_fail: "Uložení selhalo — zkontrolujte hodnoty.",
    u_add: "Přidat uživatele",
    u_pw_title: "Změna hesla",
    u_pw_old: "Současné heslo", u_pw_new: "Nové heslo",
    pw_saved: "Heslo změněno ✓.", pw_fail: "Změna selhala.",
    u_saved: "Uživatel přidán ✓.", u_del: "×",
    generic_fail: "Selhalo."
  }
};

function getCookie(name) {
  const m = document.cookie.match("(?:^|;\\s*)" + name + "=(\\w{2})");
  return m ? m[1] : null;
}

let LANG = getCookie("mclang");
if (!I18N[LANG]) LANG = "en";

function t(key) {
  return (I18N[LANG] && I18N[LANG][key] !== undefined)
    ? I18N[LANG][key] : I18N.en[key];
}

function tf(key, params) {
  return String(t(key)).replace(/\{(\w+)\}/g, (_, k) => params[k]);
}

function applyLang() {
  document.documentElement.lang = LANG;
  document.querySelectorAll("[data-i18n]").forEach(el => {
    el.textContent = t(el.dataset.i18n);
  });
  document.querySelectorAll("[data-i18n-ph]").forEach(el => {
    el.placeholder = t(el.dataset.i18nPh);
  });
  document.getElementById("langBtn").textContent =
    LANG === "cs" ? "🇨🇿 CS" : "🇬🇧 EN";
  document.getElementById("search").placeholder = t("search_ph");
  renderFilters();
  renderPower();
}

function setLang(l) {
  LANG = l;
  document.cookie = "mclang=" + l + ";path=/;max-age=31536000;samesite=lax";
  applyLang();
  refresh();
}

// ------------------------------------------------------------------- data
const FILTERS = [
  ["all", "f_all"], ["recv-mc", "f_recv_mc"], ["recv-mt", "f_recv_mt"],
  ["fwd", "f_fwd"], ["drop", "f_drop"], ["state", "f_state"]];
let active = "all";
let me = null;                            // prihlaseny uzivatel (null = login)
let power = { on: null };                 // systemd stav bridge (null = nevim)
let powerError = null;
let channels = { mc: [], mt: [], mappings: [] };
let mapDirty = false;
// feed: SQLite (/api/feed) -- strankovani + hledani
const PAGE_SIZE = 25;
let feed = { rows: [], total: 0 };
let page = 0;                             // 0-based
let search = "";
let searchTimer = null;

const EVMAP = {
  "recv":   e => e.side === "mc" ? "recv-mc" : "recv-mt",
  "forward":e => "fwd",
  "dropped":e => "drop",
  "queued": e => "q",
  "flush":  e => "fwd",
  "mc_repeat": e => "rep",
  "mc_confirm": e => "rep",
  "connect": e => "state", "disconnect": e => "state",
  "maintenance": e => "state",
  "health": e => "state",
};

function esc(s) {
  return String(s).replace(/[&<>"']/g,
    c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  const p = n => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate())
    + " " + d.toLocaleTimeString("cs-CZ");
}

function renderChannelInfo() {
  const p = document.getElementById("channelPanel");
  if (!CHANNEL_INFO || !Array.isArray(CHANNEL_INFO.rows)) { p.remove(); return; }
  let html = "<h2>" + esc(CHANNEL_INFO.title || "Bridged channel") + "</h2>";
  if (CHANNEL_INFO.hint)
    html += '<p class="hint">' + esc(CHANNEL_INFO.hint) + "</p>";
  for (const r of CHANNEL_INFO.rows) {
    html += '<div class="chrow"><span class="tag ' + (r.side === "mt" ? "mt" : "mc")
      + '">' + esc((r.side || "").toUpperCase()) + "</span> "
      + esc(r.label || "") + ' / <code>' + esc(r.value || "") + "</code>"
      + (r.note ? ' <span class="hex">(' + esc(r.note) + ")</span>" : "")
      + "</div>";
  }
  p.innerHTML = html;
  p.hidden = false;
}

function badgeClass(ev) {
  const k = EVMAP[ev.event] ? EVMAP[ev.event](ev) : "state";
  if (k === "recv-mc") return "badge recv-mc";
  if (k === "recv-mt") return "badge recv-mt";
  if (k === "fwd") return ev.direction === "mc_to_mt" ? "badge fwd-mt" : "badge fwd-mc";
  if (k === "rep") return "badge rep";
  if (k === "drop") return "badge drop";
  if (k === "q") return "badge q";
  return "badge st";
}

function badgeText(ev) {
  switch (ev.event) {
    case "recv": return ev.side === "mc" ? t("b_recv_mc") : t("b_recv_mt");
    case "forward": return ev.direction === "mc_to_mt" ? t("b_fwd_mt") : t("b_fwd_mc");
    case "dropped": return t("b_drop") + (ev.reason || "?");
    case "queued": return t("b_queued");
    case "flush": return t("b_flush");
    case "connect": return (ev.side||"") + t("b_connect");
    case "disconnect": return (ev.side||"") + t("b_disconnect");
    case "maintenance":
      return tf("b_maint", {mc: ev.mc_contacts_removed||0,
                            mt: ev.mt_nodes_removed||0});
    case "mc_repeat":
      return tf("b_repeat", {snr: ev.snr, rssi: ev.rssi, delay: ev.delay_s});
    case "mc_confirm":
      return ev.mc_tx === true ? t("b_conf_tx")
           : ev.mc_tx === false ? t("b_conf_no") : t("b_conf_na");
    case "health": return t("b_health");
    default: return ev.event;
  }
}

function filterKey(ev) {
  const k = EVMAP[ev.event] ? EVMAP[ev.event](ev) : "state";
  if (k === "fwd") return "fwd";
  if (k === "rep") return "fwd";     // repeat/potvrzeni patri k "Forwarded"
  if (k === "q") return "state";
  return k;
}

function renderCards(st) {
  const s = st.stats || {};
  const conn = on => power.on === false
    ? '<span class="dot"></span>' + esc(t("bridge_off"))
    : (on ? '<span class="dot on"></span>' + esc(t("conn_on"))
          : '<span class="dot off"></span>' + esc(t("conn_off")));
  const cards = [
    [t("card_mc"), conn(st.mc_connected)],
    [t("card_mt"), conn(st.mt_connected)],
    [t("card_fwd"), s.mc_to_mt ?? 0],
    [t("card_back"), s.mt_to_mc ?? 0],
    [t("card_queues"), (st.queue_mc||0) + " / " + (st.queue_mt||0)],
    [t("card_maint_mc"), (s.mc_contacts_removed||0) + esc(t("contacts"))],
    [t("card_maint_mt"), (s.mt_nodes_removed||0) + esc(t("nodes"))],
  ];
  document.getElementById("cards").innerHTML = cards.map(c =>
    '<div class="card"><div class="k">' + esc(c[0]) + '</div>'
    + '<div class="v">' + c[1] + '</div></div>'
  ).join("");
}

function renderRows(events) {
  const list = document.getElementById("list");
  list.innerHTML = events.map(ev => {
    if (!filterMatch(ev)) return "";
    let extra = ev.src ? ' <span class="src">' + esc(t("from"))
      + esc(ev.src) + '</span>' : "";
    return '<div class="row"><span class="t">' + fmtTime(ev.ts)
      + '</span><span class="' + badgeClass(ev) + '">' + esc(badgeText(ev))
      + '</span><span class="txt">' + esc(ev.text || "") + extra
      + '</span></div>';
  }).join("");
}

function filterMatch(ev) {
  if (active === "all") return true;
  return filterKey(ev) === active;
}

function renderFilters() {
  const f = document.getElementById("filters");
  if (!f) return;
  f.innerHTML = FILTERS.map(([k, key]) =>
    '<button data-k="' + k + '"' + (k === active ? ' class="active"' : "")
    + '>' + esc(t(key)) + '</button>').join("");
}

function renderPower() {
  const b = document.getElementById("powerBtn");
  b.classList.toggle("on", power.on === true);
  b.classList.toggle("off", power.on !== true);
  document.getElementById("powerTxt").textContent =
    powerError ? (t("power_err") + " " + powerError)
    : power.on === null ? "…"
    : (power.on ? t("power_on") : t("power_off"));
  b.hidden = !(me && me.is_admin);   // vypinac bridge jen pro admina
}

async function togglePower() {
  const b = document.getElementById("powerBtn");
  if (power.on === null || b.disabled) return;
  const target = !power.on;
  b.disabled = true;
  document.getElementById("powerTxt").textContent =
    target ? t("power_on_cmd") : t("power_off_cmd");
  try {
    const r = await fetch("/api/power", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ on: target }) });
    const j = await r.json();
    powerError = j.ok ? null : (j.error || "systemctl selhal");
    if (!j.ok) { renderPower(); b.disabled = false; return; }
    const deadline = Date.now() + 45000;
    while (Date.now() < deadline) {
      await new Promise(r => setTimeout(r, 2000));
      const r = await fetch("/api/power");
      power = await r.json();
      if (power.on === target) break;
    }
  } catch (e) { /* zustava stav, co hlasi /api/power */ }
  b.disabled = false;
  renderPower();
  refresh();
}

function chSelect(side, val) {
  const list = channels[side] || [];
  if (!list.length)
    return '<input type="number" class="mnum ' + side + '" min="0" max="'
      + (side === "mt" ? 7 : 63) + '" value="' + val + '">';
  const idxs = list.map(c => c.idx);
  if (!idxs.includes(val)) idxs.push(val);
  idxs.sort((a, b) => a - b);
  return '<select class="msel ' + side + '">' + idxs.map(i => {
    const c = list.find(x => x.idx === i);
    return '<option value="' + i + '"' + (i === val ? " selected" : "") + '>'
      + i + " · " + esc(c && c.name ? c.name : t("unnamed")) + '</option>';
  }).join("") + '</select>';
}

function mapRow(mc, mt) {
  return '<div class="mrow">MC ' + chSelect("mc", mc)
    + ' <span class="arr">▸◂</span> MT ' + chSelect("mt", mt)
    + '<button class="mrm" title="odebrat mapování">×</button></div>';
}

function renderMappings() {
  const rows = (channels.mappings || []).slice();
  document.getElementById("mapRows").innerHTML =
    rows.map(p => mapRow(p[0], p[1])).join("");
  mapDirty = false;
}

async function loadChannels() {
  try {
    const r = await fetch("/api/channels");
    const data = await r.json();
    channels.mc = data.mc || [];
    channels.mt = data.mt || [];
    channels.mappings = data.mappings || [];
    if (!mapDirty) renderMappings();
  } catch (e) { /* pri startu muze byt prazdno */ }
}

function collectMappings() {
  const out = [];
  for (const r of document.querySelectorAll("#mapRows .mrow")) {
    const mc = r.querySelector(".mc"), mt = r.querySelector(".mt");
    out.push([parseInt(mc.value, 10), parseInt(mt.value, 10)]);
  }
  return out;
}

async function saveMappings() {
  const note = document.getElementById("mapNote");
  const pairs = collectMappings();
  const mcs = pairs.map(p => p[0]), mts = pairs.map(p => p[1]);
  if (new Set(mcs).size !== mcs.length || new Set(mts).size !== mts.length) {
    note.className = "warn";
    note.textContent = t("map_dup");
    return;
  }
  try {
    const r = await fetch("/api/mappings", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ mappings: pairs }) });
    const j = await r.json();
    if (!j.ok) { note.className = "warn"; note.textContent = j.error; return; }
    channels.mappings = j.mappings;
    mapDirty = false;
    note.className = "ok";
    note.textContent = t("map_saved_ok");
    setTimeout(() => { note.textContent = ""; }, 20000);
  } catch (e) {
    note.className = "warn";
    note.textContent = t("map_save_fail");
  }
}

// ----------------------------------------------------------------- feed
async function loadFeed() {
  try {
    const params = new URLSearchParams({ offset: page * PAGE_SIZE,
      limit: PAGE_SIZE, q: search, filter: active });
    const r = await fetch("/api/feed?" + params);
    feed = await r.json();
    renderRows(feed.rows);
    renderPager();
  } catch (e) { /* db jeste neexistuje */ }
}

function renderPager() {
  const pages = Math.max(1, Math.ceil((feed.total || 0) / PAGE_SIZE));
  if (page >= pages) page = pages - 1;
  document.getElementById("pager").innerHTML =
    '<button data-p="prev"' + (page === 0 ? " disabled" : "") + '>‹</button>'
    + ' <span>' + (page + 1) + " / " + pages + '</span>'
    + ' <button data-p="next"' + (page >= pages - 1 ? " disabled" : "")
    + '>›</button>'
    + ' <span>(' + (feed.total || 0) + ')</span>';
}

function chartBars(days, series, pct) {
  let max = 1;
  if (!pct)
    for (const s of series)
      for (const v of (s.values || [])) if (v > max) max = v;
  return '<div class="bars">' + days.map((d, i) => {
    const tip = d + ": " + series.map(s =>
      (s.name ? s.name + " " : "") + (s.values[i] ?? 0)).join(", ");
    return '<div class="col" title="' + esc(tip) + '">'
      + series.map(s => {
        const v = s.values[i] ?? 0;
        const h = Math.max(1, Math.round(pct ? v / 100 * 70 : v / max * 70));
        return '<div class="bar ' + s.cls + '" style="height:' + h + 'px"></div>';
      }).join("") + '</div>';
  }).join("") + "</div>";
}

function renderCharts(h) {
  const p = document.getElementById("statsPanel");
  if (!h || !Array.isArray(h.days) || !h.days.length) { p.hidden = true; return; }
  p.hidden = false;
  document.getElementById("charts").innerHTML =
    '<div class="chart"><h3>' + esc(t("c_msgs")) + '</h3>'
    + chartBars(h.days, [{cls: "mc", name: "MC→MT", values: h.mc_to_mt},
                         {cls: "mt", name: "MT→MC", values: h.mt_to_mc}])
    + '</div>'
    + '<div class="chart"><h3>' + esc(t("c_recon")) + '</h3>'
    + chartBars(h.days, [{cls: "warn", values: h.reconnects}]) + '</div>'
    + '<div class="chart"><h3>' + esc(t("c_uptime")) + '</h3>'
    + chartBars(h.days, [{cls: "mc", name: "MC", values: h.mc_up},
                         {cls: "mt", name: "MT", values: h.mt_up}], true)
    + '</div>';
}

async function loadHistory() {
  try {
    const r = await fetch("/api/history?days=30");
    renderCharts(await r.json());
  } catch (e) { /* db jeste neexistuje */ }
}

// ----------------------------------------------------------------- auth
async function checkAuth() {
  const r = await fetch("/api/me");
  if (r.status === 401) {
    me = null;
    document.getElementById("loginOverlay").hidden = false;
    return false;
  }
  me = await r.json();
  document.getElementById("loginOverlay").hidden = true;
  document.getElementById("who").textContent = "@" + me.username;
  document.getElementById("logoutBtn").hidden = false;
  document.getElementById("usersTab").hidden = !me.is_admin;
  renderPower();
  return true;
}

async function doLogin() {
  const err = document.getElementById("liErr");
  err.textContent = "";
  try {
    const r = await fetch("/api/login", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username: liUser.value,
                             password: liPass.value }) });
    const j = await r.json();
    if (!j.ok) { err.textContent = t("li_err"); return; }
    liPass.value = "";
    document.getElementById("defPwWarn").hidden = !j.default_pw;
    if (await checkAuth()) { loadConfig(); loadUsers(); refresh(); }
  } catch (e) { err.textContent = t("generic_fail"); }
}

async function doLogout() {
  try { await fetch("/api/logout", { method: "POST" }); } catch (e) {}
  me = null;
  location.reload();
}

// ----------------------------------------------------------------- config
async function loadConfig() {
  if (!me || !me.is_admin) return;
  const r = await fetch("/api/config");
  if (!r.ok) return;
  const j = await r.json();
  const host = document.getElementById("cfgForm");
  let html = "";
  for (const sec of j.sections) {
    html += "<h3>" + esc(sec.name) + "</h3>";
    for (const key of sec.keys) {
      const m = j.meta[key] || {};
      const val = j.values[key] ?? "";
      let field;
      if (m.type === "bool") {
        field = '<input type="checkbox" data-key="' + key + '"'
                + (val ? " checked" : "") + '>';
      } else if (m.type === "enum") {
        field = '<select data-key="' + key + '">' + (m.choices || []).map(c =>
          '<option value="' + esc(c) + '"' + (val === c ? " selected" : "")
          + ">" + esc(c) + "</option>").join("") + "</select>";
      } else if (m.type === "int" || m.type === "float") {
        field = '<input type="number" step="any" data-key="' + key
          + '" value="' + esc(val) + '">'
          + (m.min !== undefined ? "" : "");
      } else {
        field = '<input type="' + (m.secret ? "password" : "text")
          + '" data-key="' + key + '" value="' + esc(val) + '">';
      }
      html += '<div class="crow"><label>' + esc(m.label || key)
        + (m.restart ? ' <span class="hex">&#8635;</span>' : "")
        + '</label>' + field + "</div>";
    }
  }
  host.innerHTML = html;
  document.getElementById("cfgNote").textContent = "";
}

async function saveConfig() {
  const note = document.getElementById("cfgNote");
  const values = {};
  document.querySelectorAll("#cfgForm [data-key]").forEach(el => {
    values[el.dataset.key] = el.type === "checkbox" ? el.checked : el.value;
  });
  try {
    const r = await fetch("/api/config", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ values }) });
    const j = await r.json();
    if (!j.ok) {
      note.className = "warn";
      note.textContent = t("cfg_save_fail") + " "
        + (j.errors ? JSON.stringify(j.errors) : (j.error || ""));
      return;
    }
    note.className = "ok";
    note.textContent = t("cfg_saved") + (j.restart && j.restart.length
      ? " " + t("cfg_restart") + " " + j.restart.join(", ") : "");
  } catch (e) {
    note.className = "warn";
    note.textContent = t("cfg_save_fail");
  }
}

// ----------------------------------------------------------------- users
async function loadUsers() {
  if (!me || !me.is_admin) return;
  const r = await fetch("/api/users");
  if (!r.ok) return;
  const j = await r.json();
  document.getElementById("usersRows").innerHTML =
    (j.users || []).map(u =>
      '<div class="urow"><span>' + esc(u.username) + "</span>"
      + (u.is_admin ? ' <span class="tag mc">admin</span>' : "")
      + ' <button class="mrm" data-del="' + u.id + '"'
      + (u.id === me.id ? " disabled" : "")
      + ' title="' + esc(t("u_add") ? "" : "") + '">&times;</button></div>'
    ).join("");
}

async function addUser() {
  const note = document.getElementById("usersNote") || {};
  try {
    const r = await fetch("/api/users", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: "create",
        username: nuName.value, password: nuPass.value,
        is_admin: nuAdmin.checked }) });
    const j = await r.json();
    if (!j.ok) { alert(j.error || "failed"); return; }
    nuName.value = ""; nuPass.value = "";
    loadUsers();
  } catch (e) { alert(t("generic_fail")); }
}

async function changePw() {
  const note = document.getElementById("pwNote");
  try {
    const r = await fetch("/api/password", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ old: pwOld.value, new: pwNew.value }) });
    const j = await r.json();
    note.textContent = j.ok ? t("pw_saved") : (j.error || t("pw_fail"));
    if (j.ok) { pwOld.value = ""; pwNew.value = ""; }
  } catch (e) { note.textContent = t("pw_fail"); }
}

// ----------------------------------------------------------------- views
let view = "dash";
function showView(v) {
  view = v;
  for (const el of document.querySelectorAll(".viewbtn"))
    el.classList.toggle("active", el.dataset.v === v);
  document.getElementById("viewDash").hidden = v !== "dash";
  document.getElementById("viewConfig").hidden = v !== "config";
  document.getElementById("viewUsers").hidden = v !== "users";
  if (v === "config") loadConfig();
  if (v === "users") loadUsers();
}

// ----------------------------------------------------------------- refresh
async function refresh() {
  if (!me) return;                       // bez prihlaseni nema data smysl
  try {
    const [str, pwr] = await Promise.all([
      fetch("/api/status"),
      fetch("/api/power")
    ]);
    const st = await str.json();
    power = await pwr.json();
    renderPower();
    renderCards(st);
    loadFeed();
    loadHistory();
    loadChannels();
    document.getElementById("refresh").textContent =
      new Date().toLocaleTimeString("cs-CZ");
  } catch (e) { /* prazdne odpovedi pri startu */ }
}

// ----------------------------------------------------------------- events
document.querySelectorAll(".viewbtn").forEach(b =>
  b.addEventListener("click", () => showView(b.dataset.v)));
document.getElementById("filters").addEventListener("click", e => {
  if (e.target.dataset.k) {
    active = e.target.dataset.k;
    page = 0;
    renderFilters();
    loadFeed();
  }
});
document.getElementById("search").addEventListener("input", e => {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => {
    search = e.target.value.trim();
    page = 0;
    loadFeed();
  }, 400);
});
document.getElementById("pager").addEventListener("click", e => {
  if (e.target.dataset.p === "prev" && page > 0) { page--; loadFeed(); }
  if (e.target.dataset.p === "next"
      && page < Math.ceil((feed.total || 0) / PAGE_SIZE) - 1) {
    page++; loadFeed();
  }
});
document.getElementById("powerBtn").addEventListener("click", togglePower);
document.getElementById("langBtn").addEventListener("click", () =>
  setLang(LANG === "cs" ? "en" : "cs"));
document.getElementById("logoutBtn").addEventListener("click", doLogout);
document.getElementById("liBtn").addEventListener("click", doLogin);
document.getElementById("liPass").addEventListener("keydown", e => {
  if (e.key === "Enter") doLogin();
});
document.getElementById("mapAdd").addEventListener("click", () => {
  document.getElementById("mapRows")
    .insertAdjacentHTML("beforeend", mapRow(0, 0));
  mapDirty = true;
});
document.getElementById("mapSave").addEventListener("click", saveMappings);
document.getElementById("cfgSave").addEventListener("click", saveConfig);
document.getElementById("nuAdd").addEventListener("click", addUser);
document.getElementById("pwSave").addEventListener("click", changePw);
document.getElementById("usersRows").addEventListener("click", async e => {
  if (!e.target.dataset.del) return;
  if (!confirm("Delete user?")) return;
  const r = await fetch("/api/users", { method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ action: "delete", id: +e.target.dataset.del }) });
  const j = await r.json();
  if (!j.ok) { alert(j.error || "failed"); }
  loadUsers();
});
document.getElementById("mapRows").addEventListener("click", e => {
  if (e.target.classList.contains("mrm")) {
    e.target.closest(".mrow").remove();
    mapDirty = true;
  }
});
document.getElementById("mapRows").addEventListener("change", () => {
  mapDirty = true;
});
applyLang();
renderChannelInfo();
checkAuth().then(ok => { if (ok) refresh(); });
setInterval(() => { if (me) refresh(); }, 5000);
</script>
</body>
</html>
"""


def load_events(limit):
    try:
        with open(EVENTS, encoding="utf-8") as f:
            lines = f.readlines()[-limit:]
        out = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        out.reverse()  # nejnovější nahoře
        return out
    except OSError:
        return []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # ztlumit stdlib access log
        pass

    # ------------------------------------------------------------- helpers

    def _send_json(self, obj, status=200, cookies=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for c in (cookies or []):
            self.send_header("Set-Cookie", c)
        self.end_headers()
        self.wfile.write(body)

    def _auth(self):
        """Aktualni uzivatel (dict) nebo None (neautorizovan)."""
        m = re.search(r"(?:^|;\s*)" + SESSION_COOKIE + r"=([A-Za-z0-9_\-]+)",
                      self.headers.get("Cookie", ""))
        if not m:
            return None
        conn = db_connect()
        try:
            row = conn.execute(
                "SELECT u.id, u.username, u.is_admin FROM sessions s"
                " JOIN users u ON u.id = s.user_id"
                " WHERE s.token = ? AND s.expires > ?",
                (m.group(1), time.time())).fetchone()
            return dict(row) if row else None
        except sqlite3.Error:
            return None
        finally:
            conn.close()

    def _origin_ok(self):
        """Same-origin kontrola pro POST (zjednoduseny CSRF ochran)."""
        origin = self.headers.get("Origin")
        if not origin:
            return True
        return urlparse(origin).netloc == (self.headers.get("Host") or "")

    def _read_body_json(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            n = 0
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            return None

    # --------------------------------------------------------------- GET

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = PAGE.replace(
                "__CHANNEL_INFO__",
                json.dumps(read_json(CHANNEL_INFO, None),
                           ensure_ascii=False)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/api/me":
            u = self._auth()
            self._send_json(u if u else {"ok": False},
                            status=200 if u else 401)
            return
        u = self._auth()
        if not u:
            self._send_json({"ok": False, "error": "unauthorized"},
                            status=401)
            return
        if parsed.path == "/api/events":
            qs = parse_qs(parsed.query)
            limit = min(int(qs.get("limit", [150])[0]), 300)
            self._send_json(load_events(limit))
        elif parsed.path == "/api/feed":
            qs = parse_qs(parsed.query)
            flt = qs.get("filter", ["all"])[0]
            q = (qs.get("q", [""])[0] or "")[:100]
            limit = min(int(qs.get("limit", [25])[0]), 100)
            offset = max(0, int(qs.get("offset", [0])[0]))
            self._send_json(query_feed(flt, q, offset, limit))
        elif parsed.path == "/api/history":
            qs = parse_qs(parsed.query)
            days = int(qs.get("days", [30])[0])
            self._send_json(query_history(days) or {})
        elif parsed.path == "/api/status":
            self._send_json(read_json(STATUS, {
                "updated": 0, "mc_connected": False,
                "mt_connected": False, "stats": {}}))
        elif parsed.path == "/api/power":
            self._send_json({"on": bridge_running()})
        elif parsed.path == "/api/channels":
            data = read_json(CHANNELS, {"mc": [], "mt": [], "updated": 0})
            conn = db_connect()
            try:
                raw = db_get_config(conn, "channels.mappings")
            except sqlite3.Error:
                raw = None
            finally:
                conn.close()
            if raw is None:   # legacy soubor (bridge ho pri startu migrates)
                data["mappings"] = read_json(
                    os.path.join(BASE, "mappings.json"), {}).get("mappings")
            else:
                try:
                    data["mappings"] = json.loads(raw)
                except json.JSONDecodeError:
                    data["mappings"] = None
            self._send_json(data)
        elif parsed.path == "/api/config":
            if not u.get("is_admin"):
                self._send_json({"ok": False, "error": "admin only"},
                                status=403)
                return
            st = read_json(STATUS, {})
            self._send_json({
                "values": st.get("config", {}),
                "meta": CONFIG_SCHEMA,
                "sections": [{"name": name, "keys": keys}
                             for name, keys in config_sections()],
            })
        elif parsed.path == "/api/users":
            if not u.get("is_admin"):
                self._send_json({"ok": False, "error": "admin only"},
                                status=403)
                return
            conn = db_connect()
            try:
                rows = conn.execute("SELECT id, username, is_admin, created"
                                     " FROM users ORDER BY id").fetchall()
            finally:
                conn.close()
            self._send_json({"users": [dict(r) for r in rows]})
        else:
            self.send_error(404)

    # --------------------------------------------------------------- POST

    def do_POST(self):
        if not self._origin_ok():
            self._send_json({"ok": False, "error": "cross-origin"},
                            status=403)
            return
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/login":
                self._post_login()
                return
            u = self._auth()
            if not u:
                self._send_json({"ok": False, "error": "unauthorized"},
                                status=401)
                return
            if parsed.path == "/api/logout":
                self._post_logout()
            elif parsed.path == "/api/power":
                self._post_power(u)
            elif parsed.path == "/api/mappings":
                self._post_mappings(u)
            elif parsed.path == "/api/config":
                self._post_config(u)
            elif parsed.path == "/api/users":
                self._post_users(u)
            elif parsed.path == "/api/password":
                self._post_password(u)
            else:
                self.send_error(404)
        except Exception as e:
            try:
                self._send_json({"ok": False, "error": repr(e)})
            except Exception:
                pass

    def _post_login(self):
        body = self._read_body_json() or {}
        username = str(body.get("username") or "")
        password = str(body.get("password") or "")
        conn = db_connect()
        try:
            row = conn.execute("SELECT * FROM users WHERE username = ?",
                               (username,)).fetchone()
        finally:
            conn.close()
        if not (row and check_pw(password, row["pw_hash"])):
            time.sleep(1)  # zpomalit hadani hesla
            self._send_json({"ok": False}, status=401)
            return
        token = secrets.token_urlsafe(32)
        conn = db_connect()
        try:
            conn.execute("DELETE FROM sessions WHERE expires < ?",
                         (time.time(),))
            conn.execute("INSERT INTO sessions (token, user_id, expires)"
                         " VALUES (?,?,?)",
                         (token, row["id"], time.time() + SESSION_MAX_AGE))
            conn.commit()
        finally:
            conn.close()
        self._send_json(
            {"ok": True, "username": row["username"],
             "is_admin": bool(row["is_admin"]),
             "default_pw": username == "admin" and password == "admin"},
            cookies=[f"{SESSION_COOKIE}={token}; Path=/; HttpOnly;"
                     f" SameSite=Lax; Max-Age={SESSION_MAX_AGE}"])

    def _post_logout(self):
        m = re.search(r"(?:^|;\s*)" + SESSION_COOKIE + r"=([A-Za-z0-9_\-]+)",
                      self.headers.get("Cookie", ""))
        if m:
            conn = db_connect()
            try:
                conn.execute("DELETE FROM sessions WHERE token = ?",
                             (m.group(1),))
                conn.commit()
            finally:
                conn.close()
        self._send_json({"ok": True},
                        cookies=[f"{SESSION_COOKIE}=; Path=/; HttpOnly;"
                                 " SameSite=Lax; Max-Age=0"])

    def _post_power(self, user):
        if not user.get("is_admin"):
            self._send_json({"ok": False, "error": "admin only"})
            return
        body = self._read_body_json()
        if not isinstance(body, dict) or not isinstance(body.get("on"), bool):
            self._send_json({"ok": False,
                            "error": "ocekavam {\"on\": true|false}"})
            return
        cmd = ["sudo", "-n", SYSTEMCTL, "--no-block",
               "start" if body["on"] else "stop", BRIDGE_UNIT]
        try:
            r = subprocess.run(cmd, capture_output=True, timeout=30)
        except (OSError, subprocess.TimeoutExpired) as e:
            self._send_json({"ok": False, "error": repr(e)})
            return
        if r.returncode != 0:
            self._send_json({"ok": False, "error": r.stderr.decode(
                "utf-8", "replace").strip() or f"systemctl exit {r.returncode}"})
            return
        self._send_json({"ok": True, "target": body["on"]})

    def _post_mappings(self, user):
        if not user.get("is_admin"):
            self._send_json({"ok": False, "error": "admin only"})
            return
        pairs = valid_mappings(self._read_body_json() or {})
        if pairs is None:
            self._send_json({"ok": False,
                            "error": "neplatne mapovani (dvojice mc:mt, "
                                     "kazdy kanal max. jednou, mt 0-7)"})
            return
        try:
            conn = db_connect()
            try:
                db_set_config(conn, "channels.mappings", json.dumps(pairs))
                conn.commit()
            finally:
                conn.close()
        except sqlite3.Error as e:
            self._send_json({"ok": False, "error": repr(e)})
            return
        self._send_json({"ok": True, "mappings": pairs})

    def _post_config(self, user):
        if not user.get("is_admin"):
            self._send_json({"ok": False, "error": "admin only"})
            return
        from config_schema import parse_value
        body = self._read_body_json() or {}
        values = body.get("values")
        if not isinstance(values, dict):
            self._send_json({"ok": False, "error": "ocekavam {values: {...}}"})
            return
        out = {"ok": True, "applied": [], "restart": [], "errors": {}}
        conn = db_connect()
        try:
            for key, raw in values.items():
                if key not in CONFIG_SCHEMA:
                    out["errors"][key] = "unknown key"
                    out["ok"] = False
                    continue
                _, err = parse_value(key, raw)
                if err:
                    out["errors"][key] = err
                    out["ok"] = False
                    continue
                conn.execute("INSERT OR REPLACE INTO config (key, value)"
                             " VALUES (?, ?)", (key, str(raw)))
                out["applied"].append(key)
                if CONFIG_SCHEMA[key].get("restart"):
                    out["restart"].append(key)
            conn.execute("INSERT OR REPLACE INTO config (key, value)"
                         " VALUES ('config.updated', ?)", (str(time.time()),))
            conn.commit()
        finally:
            conn.close()
        self._send_json(out)

    def _post_users(self, user):
        if not user.get("is_admin"):
            self._send_json({"ok": False, "error": "admin only"})
            return
        body = self._read_body_json() or {}
        action = body.get("action")
        conn = db_connect()
        try:
            if action == "create":
                name = str(body.get("username") or "").strip()
                pw = str(body.get("password") or "")
                if not re.fullmatch(r"[A-Za-z0-9_.\-]{2,32}", name):
                    self._send_json({"ok": False,
                                    "error": "username 2-32 znaku [A-Za-z0-9_.-]"})
                    return
                if len(pw) < 6:
                    self._send_json({"ok": False,
                                    "error": "heslo min. 6 znaku"})
                    return
                try:
                    conn.execute("INSERT INTO users (username, pw_hash,"
                                 " is_admin, created) VALUES (?,?,?,?)",
                                 (name, hash_pw(pw),
                                  1 if body.get("is_admin") else 0,
                                  time.time()))
                    conn.commit()
                except sqlite3.IntegrityError:
                    self._send_json({"ok": False,
                                    "error": "uzivatel uz existuje"})
                    return
                self._send_json({"ok": True})
            elif action == "delete":
                uid = int(body.get("id") or 0)
                if uid == user.get("id"):
                    self._send_json({"ok": False,
                                    "error": "nelze smazat vlastni ucet"})
                    return
                victim = conn.execute("SELECT is_admin FROM users WHERE id=?",
                                      (uid,)).fetchone()
                if victim and victim["is_admin"]:
                    admins = conn.execute("SELECT COUNT(*) FROM users"
                                           " WHERE is_admin=1").fetchone()[0]
                    if admins <= 1:
                        self._send_json({"ok": False,
                                        "error": "nelze smazat posledniho admina"})
                        return
                conn.execute("DELETE FROM users WHERE id=?", (uid,))
                conn.execute("DELETE FROM sessions WHERE user_id=?", (uid,))
                conn.commit()
                self._send_json({"ok": True})
            else:
                self._send_json({"ok": False, "error": "neznama akce"})
        finally:
            conn.close()

    def _post_password(self, user):
        body = self._read_body_json() or {}
        old = str(body.get("old") or "")
        new = str(body.get("new") or "")
        if len(new) < 6:
            self._send_json({"ok": False, "error": "heslo min. 6 znaku"})
            return
        conn = db_connect()
        try:
            row = conn.execute("SELECT pw_hash FROM users WHERE id=?",
                               (user["id"],)).fetchone()
            if not (row and check_pw(old, row["pw_hash"])):
                self._send_json({"ok": False, "error": "soucasne hesle nesouhlasi"})
                return
            conn.execute("UPDATE users SET pw_hash=? WHERE id=?",
                         (hash_pw(new), user["id"]))
            conn.commit()
        finally:
            conn.close()
        self._send_json({"ok": True})


if __name__ == "__main__":
    init_db()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Mesh bridge dashboard: http://0.0.0.0:{PORT}/")
    srv.serve_forever()
