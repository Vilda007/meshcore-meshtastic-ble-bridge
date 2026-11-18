#!/usr/bin/env python3
"""Web dashboard pro MeshCore <-> Meshtastic bridge (bezi na Raspberry Pi).

Cte events.jsonl (event log z bridge.py) a status.json; zadne externi
zavislosti (jen stdlib). Port 8420.

  http://<pi-ip>:8420/
"""

import json
import os
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

BASE = os.path.dirname(os.path.abspath(__file__))
EVENTS = os.path.join(BASE, "events.jsonl")
STATUS = os.path.join(BASE, "status.json")
MAPPINGS = os.path.join(BASE, "mappings.json")
CHANNELS = os.path.join(BASE, "channels.json")
# Volitelny soubor s pristupovymi udaji bridgovaneho kanalu (napr. klic
# kanalu MeshCore). Neni-li pritomen, dashboard panel s udaji nezobrazi.
# NEPOSOILET DO repozitare -- obsahuje klice! (viz channel_info.json.example)
CHANNEL_INFO = os.path.join(BASE, "channel_info.json")
PORT = 8420

# vypinac bridge: systemd pres usokene sudoers pravidlo (viz sudoers.d-mesh-bridge)
BRIDGE_UNIT = "mesh-bridge"
SYSTEMCTL = "/usr/bin/systemctl"


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


def valid_mappings(data):
    """Validace tela POST /api/mappings -> [[mc, mt], ...] nebo None.

    Kazdy MC kanal i MT kanal muze byt ve mapovani maximalne jednou --
    mapa musi byt oboustranne jednoznacna (jeden cil na jednu stranu).
    """
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

PAGE = """<!doctype html>
<html lang="cs">
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
h1 { font-size:18px; margin:0 0 12px; display:flex; gap:10px;
     align-items:center; flex-wrap:wrap; }
h1 .sub { color:var(--dim); font-weight:400; font-size:13px; }
.cards { display:flex; gap:10px; flex-wrap:wrap; margin-bottom:14px; }
.card { background:var(--panel); border:1px solid var(--line);
        border-radius:10px; padding:10px 14px; min-width:150px; }
.card .k { color:var(--dim); font-size:11px; text-transform:uppercase;
           letter-spacing:.05em; }
.card .v { font-size:18px; font-weight:600; }
#powerBtn { display:inline-flex; align-items:center; gap:7px; cursor:pointer;
  font:600 13px/1 inherit; padding:7px 14px; border-radius:20px;
  border:1px solid var(--line); background:var(--panel); color:var(--fg); }
#powerBtn.on { border-color:var(--con); color:var(--con); }
#powerBtn.off { border-color:var(--dis); color:var(--dis); }
#powerBtn:disabled { opacity:.5; cursor:wait; }
#powerBtn .pk { width:10px; height:10px; border-radius:50%; }
#powerBtn.on .pk { background:var(--con); }
#powerBtn.off .pk { background:var(--dis); }
.panel { background:var(--panel); border:1px solid var(--line);
  border-radius:10px; padding:12px 14px; margin-bottom:14px; }
.panel h2 { font-size:14px; margin:0 0 4px; }
.panel .hint { color:var(--dim); font-size:12px; margin:0 0 10px; }
.mrow { display:flex; gap:8px; align-items:center; margin-bottom:8px;
  flex-wrap:wrap; }
.mrow .arr { color:var(--dim); font-size:12px; }
.msel, .mnum { background:var(--bg); color:var(--fg); border:1px solid var(--line);
  border-radius:6px; padding:5px 8px; font-size:13px; max-width:220px; }
.msel.mc { border-color:rgba(126,196,110,.4); }
.msel.mt { border-color:rgba(110,168,216,.4); }
.mnum { width:70px; }
.mrm { background:none; border:none; color:var(--drop); font-size:16px;
  cursor:pointer; padding:2px 8px; }
.mbtn { background:var(--panel); color:var(--fg); border:1px solid var(--line);
  border-radius:20px; padding:5px 14px; cursor:pointer; font-size:12px; }
.mbtn.primary { border-color:var(--mt); color:var(--mt); font-weight:600; }
#mapNote { font-size:12px; margin-left:10px; }
#mapNote.ok { color:var(--con); }
#mapNote.warn { color:var(--warn); }
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
.chrow .hex { color:var(--dim); font-size:12px; }
.tag { font-size:10px; font-weight:700; padding:2px 7px;
  border-radius:4px; white-space:nowrap; letter-spacing:.03em; }
.tag.mc { background:rgba(126,196,110,.15); color:var(--mc); }
.tag.mt { background:rgba(110,168,216,.15); color:var(--mt); }
footer { color:var(--dim); font-size:12px; margin-top:16px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>Mesh bridge <span class="sub">MeshCore &harr; Meshtastic</span>
      <button id="powerBtn" class="off"><span class="pk"></span>
      <span id="powerTxt">…</span></button></h1>
  <div class="cards" id="cards"></div>
  <div class="panel" id="mapPanel">
    <h2>Mapov&aacute;n&iacute; kan&aacute;lů</h2>
    <p class="hint">Každý MeshCore kan&aacute;l se přepos&iacute;l&aacute; na p&aacute;rovany
      Meshtastic kan&aacute;l a zpět. Změny bridge přebere do ~15 s
      (nebo při př&iacute;šte startu). Jeden kan&aacute;l může b&yacute;t
      v mapov&aacute;n&iacute; jen jednou. N&aacute;zvy kan&aacute;lů se načtou
      samy, až se bridge připoj&iacute; k oběma zař&iacute;zen&iacute;m
      &mdash; dokud je bridge vypnut&yacute; nebo n&aacute;zvy nezn&aacute;,
      zad&aacute;vaj se č&iacute;sla indexů kan&aacute;lů.</p>
    <div id="mapRows"></div>
    <button class="mbtn" id="mapAdd">+ přidat mapov&aacute;n&iacute;</button>
    <button class="mbtn primary" id="mapSave">Uložit</button>
    <span id="mapNote"></span>
  </div>
  <div class="panel" id="channelPanel" hidden></div>
  <div class="filters" id="filters"></div>
  <div id="list"></div>
  <footer>Posledn&iacute; obnoven&iacute;: <span id="refresh"></span>
   &middot; bridge na Raspberry Pi &middot; log: events.jsonl</footer>
</div>
<script>
// pristupove udaje bridgovaneho kanalu (channel_info.json na serveru);
// null = soubor neexistuje, panel se nezobrazi
const CHANNEL_INFO = __CHANNEL_INFO__;

const FILTERS = [
  ["all", "Vše"], ["recv-mc", "Př&iacute;jem MC"],
  ["recv-mt", "Př&iacute;jem MT"], ["fwd", "Předan&eacute;"],
  ["drop", "Filtrovan&eacute;"], ["state", "Stavy"]];
let active = "all";
let lastTs = 0;
let power = { on: null };               // systemd stav bridge (null = nevim)
let powerError = null;                   // chybova hlaska posledniho prepnuti
let channels = { mc: [], mt: [], mappings: [] };
let mapDirty = false;

const EVMAP = {
  "recv":   e => e.side === "mc" ? "recv-mc" : "recv-mt",
  "forward":e => e.direction === "mc_to_mt" ? "fwd" : "fwd",
  "dropped":e => "drop",
  "queued": e => "q",
  "flush":  e => "fwd",
  "connect": e => "state", "disconnect": e => "state",
  "maintenance": e => "state",
};

function esc(s) {
  return String(s).replace(/[&<>"']/g,
    c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
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

function fmtTime(ts) {
  const d = new Date(ts * 1000);
  const p = n => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate())
    + " " + d.toLocaleTimeString("cs-CZ");
}

function badgeClass(ev) {
  const k = EVMAP[ev.event] ? EVMAP[ev.event](ev) : "state";
  if (k === "recv-mc") return "badge recv-mc";
  if (k === "recv-mt") return "badge recv-mt";
  if (k === "fwd") return ev.direction === "mc_to_mt" ? "badge fwd-mt" : "badge fwd-mc";
  if (k === "drop") return "badge drop";
  if (k === "q") return "badge q";
  return "badge st";
}

function badgeText(ev) {
  switch (ev.event) {
    case "recv": return ev.side === "mc" ? "MC ▸ příjem"
                                         : "MT ▸ příjem";
    case "forward": return ev.direction === "mc_to_mt" ? "MC ▸▸ MT" : "MT ▸▸ MC";
    case "dropped": return "filtr: " + (ev.reason || "?");
    case "queued": return "ve frontě";
    case "flush": return "vypláceno z fronty";
    case "connect": return (ev.side||"") + " připojeno";
    case "disconnect": return (ev.side||"") + " odpojeno";
    case "maintenance": return "údržba: MC -" + (ev.mc_contacts_removed||0)
      + " kontaktů, MT -" + (ev.mt_nodes_removed||0) + " uzlů";
    default: return ev.event;
  }
}

function filterKey(ev) {
  const k = EVMAP[ev.event] ? EVMAP[ev.event](ev) : "state";
  if (k === "fwd") return "fwd";
  if (k === "q") return "state";
  return k;
}

function renderCards(st) {
  const s = st.stats || {};
  const conn = on => power.on === false
    ? '<span class="dot"></span>vypnuto'
    : (on ? '<span class="dot on"></span>připojeno'
          : '<span class="dot off"></span>odpojeno');
  const cards = [
    ["MeshCore", conn(st.mc_connected)],
    ["Meshtastic", conn(st.mt_connected)],
    ["MC▸MT", s.mc_to_mt ?? 0],
    ["MT▸MC", s.mt_to_mc ?? 0],
    ["Fronty", (st.queue_mc||0) + " / " + (st.queue_mt||0)],
    ["Údržba: smazáno", (s.mc_contacts_removed||0) + " kontaktů, "
       + (s.mt_nodes_removed||0) + " uzlů"],
  ];
  document.getElementById("cards").innerHTML = cards.map(c =>
    '<div class="card"><div class="k">' + c[0] + '</div>'
    + '<div class="v">' + (c[1] instanceof String || typeof c[1] === "string"
        ? c[1] : c[1]) + '</div></div>').join("");
}

function renderRows(events) {
  const list = document.getElementById("list");
  list.innerHTML = events.map(ev => {
    if (!filterMatch(ev)) return "";
    let extra = ev.src ? ' <span class="src">od ' + esc(ev.src) + '</span>' : "";
    return '<div class="row"><span class="t">' + fmtTime(ev.ts)
      + '</span><span class="' + badgeClass(ev) + '">' + esc(badgeText(ev))
      + '</span><span class="txt">' + esc(ev.text || "") + extra
      + '</span></div>';
  }).join("");
}

function filterMatch(ev) {
  if (active === "all") return true;
  if (active === "fwd") return filterKey(ev) === "fwd";
  if (active === "drop") return filterKey(ev) === "drop";
  if (active === "recv-mc") return filterKey(ev) === "recv-mc";
  if (active === "recv-mt") return filterKey(ev) === "recv-mt";
  if (active === "state") return filterKey(ev) === "state";
  return true;
}

function renderPower() {
  const b = document.getElementById("powerBtn");
  b.classList.toggle("on", power.on === true);
  b.classList.toggle("off", power.on !== true);
  document.getElementById("powerTxt").textContent =
    powerError ? ("⚠ " + powerError)
    : power.on === null ? "…"
    : (power.on ? "běží · vypnout" : "vypnut · zapnout");
}

async function togglePower() {
  const b = document.getElementById("powerBtn");
  if (power.on === null || b.disabled) return;
  const target = !power.on;
  b.disabled = true;
  document.getElementById("powerTxt").textContent =
    target ? "zapínám…" : "vypínám…";
  try {
    const r = await fetch("/api/power", { method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ on: target }) });
    const j = await r.json();
    powerError = j.ok ? null : (j.error || "systemctl selhal");
    if (!j.ok) { renderPower(); b.disabled = false; return; }
    // start/stop bezi na pozadi (--no-block); cekej, az unita prejde
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
  if (!list.length)  // nezname jmena (bridge jeste nenacetl) => cislem
    return '<input type="number" class="mnum ' + side + '" min="0" max="'
      + (side === "mt" ? 7 : 63) + '" value="' + val + '">';
  const idxs = list.map(c => c.idx);
  if (!idxs.includes(val)) idxs.push(val);
  idxs.sort((a, b) => a - b);
  return '<select class="msel ' + side + '">' + idxs.map(i => {
    const c = list.find(x => x.idx === i);
    return '<option value="' + i + '"' + (i === val ? " selected" : "") + '>'
      + i + " · " + esc(c && c.name ? c.name : "(bezejmenný)") + '</option>';
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
    rows.map(p => mapRow(p[0], p[1])).join("")
    || '<p class="hint" style="margin:0 0 8px">Žádné mapování — zprávy se '
       + 'nepřeposílají.</p>';
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
    note.textContent = "Každý kanál může být v mapování jen jednou.";
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
    note.textContent = power.on
      ? "Uloženo ✓ — bridge přebere do ~15 s."
      : "Uloženo ✓ — bridge převezme při startu.";
    setTimeout(() => { note.textContent = ""; }, 20000);
  } catch (e) {
    note.className = "warn";
    note.textContent = "Uložení selhalo.";
  }
}

async function refresh() {
  try {
    const [evr, str, pwr] = await Promise.all([
      fetch("/api/events?limit=150"),
      fetch("/api/status"),
      fetch("/api/power")
    ]);
    const events = await evr.json();
    const st = await str.json();
    power = await pwr.json();
    renderPower();
    renderCards(st);
    renderRows(events);
    loadChannels();  // az bridge objevi nazvy kanalu, UI samo prejde na selecty
    document.getElementById("refresh").textContent =
      new Date().toLocaleTimeString("cs-CZ");
  } catch (e) { /* prazdne odpovedi pri startu */ }
}

const f = document.getElementById("filters");
f.innerHTML = FILTERS.map(([k, label]) =>
  '<button data-k="' + k + '">' + label + '</button>').join("");
f.addEventListener("click", e => {
  if (e.target.dataset.k) {
    active = e.target.dataset.k;
    f.querySelectorAll("button").forEach(b =>
      b.classList.toggle("active", b.dataset.k === active));
    refresh();
  }
});
f.querySelector("button").classList.add("active");
document.getElementById("powerBtn").addEventListener("click", togglePower);
document.getElementById("mapAdd").addEventListener("click", () => {
  document.getElementById("mapRows")
    .insertAdjacentHTML("beforeend", mapRow(0, 0));
  mapDirty = true;
});
document.getElementById("mapSave").addEventListener("click", saveMappings);
document.getElementById("mapRows").addEventListener("click", e => {
  if (e.target.classList.contains("mrm")) {
    e.target.closest(".mrow").remove();
    mapDirty = true;
  }
});
document.getElementById("mapRows").addEventListener("change", () => {
  mapDirty = true;
});
loadChannels();
renderChannelInfo();
refresh();
setInterval(refresh, 5000);
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

    def _send_json(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/events":
            qs = parse_qs(parsed.query)
            limit = min(int(qs.get("limit", [150])[0]), 300)
            self._send_json(load_events(limit))
        elif parsed.path == "/api/status":
            self._send_json(read_json(STATUS, {
                "updated": 0, "mc_connected": False,
                "mt_connected": False, "stats": {}}))
        elif parsed.path == "/api/power":
            self._send_json({"on": bridge_running()})
        elif parsed.path == "/api/channels":
            data = read_json(CHANNELS, {"mc": [], "mt": [], "updated": 0})
            data["mappings"] = read_json(
                MAPPINGS, {}).get("mappings", None)
            self._send_json(data)
        elif parsed.path == "/":
            body = PAGE.replace(
                "__CHANNEL_INFO__",
                json.dumps(read_json(CHANNEL_INFO, None),
                           ensure_ascii=False)).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_error(404)

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/power":
                self._post_power()
            elif parsed.path == "/api/mappings":
                self._post_mappings()
            else:
                self.send_error(404)
        except Exception as e:
            try:
                self._send_json({"ok": False, "error": repr(e)})
            except Exception:
                pass

    def _read_body_json(self):
        try:
            n = int(self.headers.get("Content-Length", 0))
        except ValueError:
            n = 0
        try:
            return json.loads(self.rfile.read(n) or b"{}")
        except json.JSONDecodeError:
            return None

    def _post_power(self):
        body = self._read_body_json()
        if not isinstance(body, dict) or not isinstance(body.get("on"), bool):
            self._send_json({"ok": False, "error": "ocekavam {\"on\": true|false}"})
            return
        # --no-block: systemd odstartuje/zastavi bridge na pozadi
        # (stop muze trvat az ~20 s, TimeoutStopSec bridge unity)
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

    def _post_mappings(self):
        pairs = valid_mappings(self._read_body_json() or {})
        if pairs is None:
            self._send_json({"ok": False,
                            "error": "neplatne mapovani (dvojice mc:mt, "
                                     "kazdy kanal max. jednou, mt 0-7)"})
            return
        try:
            write_json_atomic(MAPPINGS,
                              {"mappings": pairs, "updated": int(time.time())})
        except OSError as e:
            self._send_json({"ok": False, "error": repr(e)})
            return
        self._send_json({"ok": True, "mappings": pairs})


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Mesh bridge dashboard: http://0.0.0.0:{PORT}/")
    srv.serve_forever()