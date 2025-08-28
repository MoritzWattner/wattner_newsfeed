import argparse
import asyncio
import contextlib
import dataclasses
import datetime as dt
import hashlib
import html
import os
import re
import json
from typing import List, Optional, Dict, Any

import httpx
from bs4 import BeautifulSoup, Comment

# ---- Konfiguration für Pfade ----
DEFAULT_STORAGE = "./data"
DEFAULT_FEEDS = "./feeds"
STATE_FILENAME = "state.json"  # wird unter storage_path abgelegt

# --- Utilities ---
def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return re.sub(r"-+", "-", value)

def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)

def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def write_text(path: str, txt: str):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        f.write(txt)

def node_label(node) -> str:
    if not node:
        return "<None>"
    tag = node.name or "<?>"
    _id = f"#{node.get('id')}" if node and node.get("id") else ""
    classes = node.get("class", []) if node else []
    cls = "." + ".".join(classes) if classes else ""
    return f"{tag}{_id}{cls}"

def append_log(line: str, path: str = "logs/selection.log"):
    ensure_dir(os.path.dirname(path))
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")

def short_hash(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()[:12]

def normalize_for_hash(text: str) -> str:
    # für Vergleich/Hash: Whitespace komprimieren
    return re.sub(r"\s+", " ", text).strip()

def split_paragraphs(text: str) -> list[str]:
    # robuste Absatzliste aus Text mit \n
    t = text.replace("\r\n", "\n").replace("\r", "\n")
    # Mehrfach-Leerzeilen zu zwei \n zusammenfassen
    t = re.sub(r"\n{3,}", "\n\n", t)
    # an Leerzeilen trennen
    parts = [p.strip() for p in t.split("\n\n")]
    # ungeeignete leere Teile raus
    return [p for p in parts if p]

def paragraphs_to_html(paragraphs: list[str]) -> str:
    # einfache, scrollbar-freie Darstellung als Fließtext-Blöcke
    return "".join(f"<p>{html.escape(p)}</p>" for p in paragraphs)

def added_paragraphs_html(old_text: str, new_text: str) -> str:
    import difflib
    old_pars = split_paragraphs(old_text)
    new_pars = split_paragraphs(new_text)
    sm = difflib.SequenceMatcher(None, old_pars, new_pars)
    added: list[str] = []
    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "insert":
            added.extend(new_pars[j1:j2])
        elif tag == "replace":
            # ersetzte Absätze komplett als "neu" ausweisen
            added.extend(new_pars[j1:j2])
    if not added:
        return "<p><em>Keine neuen Absätze erkennbar.</em></p>"
    return paragraphs_to_html(added)

# --- RSS minimal (no external deps) ---
def rss_escape(s: str) -> str:
    return html.escape(s, quote=True)

def make_rss(channel_title: str, channel_link: str, channel_desc: str, items: List[Dict[str, str]], *, last_build_date: Optional[str] = None) -> str:
    rss = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<rss version="2.0">',
           "<channel>",
           f"<title>{rss_escape(channel_title)}</title>",
           f"<link>{rss_escape(channel_link)}</link>",
           f"<description>{rss_escape(channel_desc)}</description>"]
    if last_build_date:
        rss.append(f"<lastBuildDate>{rss_escape(last_build_date)}</lastBuildDate>")
    for it in items:
        desc = it.get("description", "")
        # WICHTIG: CDATA sicher wrappen
        desc_cdata = cdata_wrap(desc)
        rss.append("<item>")
        rss.append(f"<title>{rss_escape(it.get('title', ''))}</title>")
        rss.append(f"<link>{rss_escape(it.get('link', ''))}</link>")
        rss.append(f"<guid isPermaLink=\"false\">{rss_escape(it.get('guid', ''))}</guid>")
        rss.append(f"<pubDate>{rss_escape(it.get('pubDate', ''))}</pubDate>")
        rss.append(f"<description>{desc_cdata}</description>")
        rss.append("</item>")
    rss.append("</channel></rss>")
    return "\n".join(rss)

def xml_sanitize(text: str) -> str:
    """
    Entfernt alle Zeichen, die in XML 1.0 nicht erlaubt sind.
    Erlaubt sind: Tab, LF, CR, U+0020..U+D7FF, U+E000..U+FFFD (und bei UCS-4 Python auch > U+10000).
    """
    if not text:
        return text
    out_chars = []
    for ch in text:
        cp = ord(ch)
        if (
            cp == 0x9 or cp == 0xA or cp == 0xD or
            (0x20 <= cp <= 0xD7FF) or
            (0xE000 <= cp <= 0xFFFD) or
            (0x10000 <= cp <= 0x10FFFF)
        ):
            out_chars.append(ch)
        # sonst: drop
    return "".join(out_chars)

def cdata_wrap(html_payload: str) -> str:
    """
    Verpackt beliebiges HTML sicher in CDATA:
    - entfernt script/style/noscript/iframe/template und HTML-Kommentare
    - entfernt ungültige XML-Zeichen
    - entschärft ']]>' innerhalb des Inhalts
    """
    if not html_payload:
        return "<![CDATA[]]>"

    try:
        soup = BeautifulSoup(html_payload, "lxml")

        # Störende Tags raus
        for bad in soup(["script", "style", "noscript", "iframe", "template"]):
            bad.decompose()

        # HTML-Kommentare entfernen (die enthalten manchmal heikle Sequenzen)
        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            c.extract()

        html_payload = str(soup)
    except Exception:
        # falls BS4 fehlschlägt, mit raw-String weiterarbeiten
        pass

    # Ungültige XML-Zeichen entfernen
    html_payload = xml_sanitize(html_payload)

    # ']]>' im Inhalt splitten, damit CDATA nicht frühzeitig endet
    html_payload = html_payload.replace("]]>", "]]]]><![CDATA[>")

    # (Optional) sichtbaren CDATA-Start neutralisieren
    html_payload = html_payload.replace("<![CDATA[", "<![C DATA[")

    return f"<![CDATA[{html_payload}]]>"

# --- Config ---
try:
    import yaml  # type: ignore
    HAVE_YAML = True
except Exception:
    HAVE_YAML = False

def load_config(path: str) -> Dict[str, Any]:
    raw = read_text(path)
    if HAVE_YAML:
        return yaml.safe_load(raw)
    # minimal YAML->dict fallback (very limited). Recommend installing PyYAML.
    import json as _json
    with contextlib.suppress(Exception):
        return _json.loads(raw)
    raise RuntimeError("Install pyyaml (pip install pyyaml) or provide JSON config.")

# --- State (statt DB) ---
def state_path(storage_path: str) -> str:
    ensure_dir(storage_path or DEFAULT_STORAGE)
    return os.path.join(storage_path or DEFAULT_STORAGE, STATE_FILENAME)

def load_state(storage_path: str) -> Dict[str, Any]:
    path = state_path(storage_path)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "sites": {},  # slug -> {name,bundesland,url,hash,excerpt,last_change}
        "items": []   # Historie der Änderungen (Events)
    }

def save_state(storage_path: str, state: Dict[str, Any]):
    path = state_path(storage_path)
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

# --- Model ---
@dataclasses.dataclass
class SiteCfg:
    name: str
    bundesland: str
    url: str
    selectors: List[str]
    mode: str = "text"  # or "html"

# --- HTTP ---
async def fetch(client: httpx.AsyncClient, url: str, timeout: int) -> Optional[str]:
    try:
        r = await client.get(url, timeout=timeout, follow_redirects=True)
        r.raise_for_status()
        r.encoding = r.encoding or "utf-8"
        return r.text
    except Exception:
        return None

# --- Diff-Helfer ---
def make_hash(text: str) -> str:
    h = hashlib.sha256()
    h.update(text.encode("utf-8", errors="ignore"))
    return h.hexdigest()

def text_diff(old: str, new: str, max_lines: int = 200) -> str:
    import difflib
    old_lines = old.split()
    new_lines = new.split()
    diff = difflib.unified_diff(old_lines, new_lines, fromfile="prev", tofile="curr", lineterm="")
    lines = list(diff)[:max_lines]
    return "<pre>" + html.escape("\n".join(lines)) + "</pre>"

def added_lines_html(old: str, new: str, max_lines: int = 80) -> str:
    import difflib
    old_lines = old.split()
    new_lines = new.split()
    added = []
    for ln in difflib.unified_diff(old_lines, new_lines, lineterm=""):
        if ln.startswith("+") and not ln.startswith("+++"):
            added.append(ln[1:])
        if len(added) >= max_lines:
            break
    if not added:
        return "<p><em>Keine reinen Hinzufügungen erkennbar.</em></p>"
    body = html.escape("\n".join(added))
    return f"<pre>{body}</pre>"

# --- Extraction + Logging ---
def extract(html_text: str, selectors: List[str], mode: str, *, site_name: str = "", site_url: str = "") -> tuple[str, Dict[str, Any]]:
    soup = BeautifulSoup(html_text, "lxml")
    # Störende Tags entfernen
    for bad in soup(["script", "style", "noscript", "iframe", "template"]):
        bad.decompose()
    # Kommentare raus
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()
    sel_list = [s.strip() for s in (selectors or []) if s and s.strip()]

    matches = []
    for sel in sel_list:
        for node in soup.select(sel):
            matches.append(node)

    if matches:
        used_strategy = f"selectors({', '.join(sel_list)})"
        used_nodes = matches
    else:
        node = soup.select_one("main")
        if node:
            used_strategy = "fallback:main"
            used_nodes = [node]
        elif soup.body:
            used_strategy = "fallback:body"
            used_nodes = [soup.body]
        else:
            used_strategy = "fallback:soup"
            used_nodes = [soup]

    # Inhalte extrahieren
    display_chunks = []
    hash_chunks = []
    for node in used_nodes:
        if mode == "html":
            t = str(node)  # HTML 1:1 übernehmen
        else:
            # Plaintext optional, aber für deine Anforderung besser auch HTML
            t = str(node)
        display_chunks.append(t)
        # Für Hash reicht auch HTML -> Plaintext
        hash_chunks.append(node.get_text(separator=" ", strip=True))

    display_text = "\n\n".join(display_chunks).strip()
    hash_text = normalize_for_hash(" ".join(hash_chunks))

    ts = now_utc().isoformat()
    node_labels = ", ".join(node_label(n) for n in used_nodes[:3])
    if len(used_nodes) > 3:
        node_labels += f" (+{len(used_nodes)-3} more)"
    selectors_pretty = "[" + ", ".join(sel_list) + "]" if sel_list else "[]"

    log_block = (
        f"[{ts}] site={site_name}\n"
        f"  url={site_url}\n"
        f"  strategy={used_strategy}\n"
        f"  selectors={selectors_pretty}\n"
        f"  matches={len(matches)}\n"
        f"  used_nodes={node_labels}\n"
        f"  text_len={len(display_text)} hash={short_hash(hash_text)}\n"
    )
    append_log(log_block)

    meta = {
        "checked_at": ts,
        "strategy": used_strategy,
        "selectors": sel_list,
        "selectors_used": sel_list if matches else [used_strategy.replace("fallback:", "(fallback: ") + ")"],
        "used_nodes": node_labels,
        "display_text": display_text,  # für Darstellung/Absätze
        "hash_text": hash_text,        # für Hash/Abgleich
    }
    # return: (anzeige-text, meta) – der anzeige-text ist mit absätzen
    return display_text, meta


# --- Sichtbare Item-Beschreibung bauen ---
def build_item_description(ev: Dict[str, Any]) -> str:
    def _fmt(ts: str) -> str:
        try:
            return rfc2822(ts)
        except Exception:
            return ts or ""

    fetched_at = ev.get('fetched_at', '')
    checked_at = ev.get('checked_at', '')
    selectors_used = ev.get('selectors_used') or ev.get('selectors') or []
    selectors_txt = ", ".join(selectors_used) if selectors_used else "—"
    used_nodes = ev.get('used_nodes', '')

    header = (
        f"<p><strong>Stand (Inhalt):</strong> {rss_escape(_fmt(fetched_at))}<br>"
        f"<strong>Zuletzt geprüft:</strong> {rss_escape(_fmt(checked_at))}<br>"
        f"<strong>Selektoren:</strong> {rss_escape(selectors_txt)}<br>"
        f"<strong>Genutzte Elemente:</strong> {rss_escape(used_nodes)}</p>"
    )

    changes_block = ""
    if ev.get("aenderungen_html"):
        changes_block = "<h3>Änderungen (neue Inhalte)</h3>" + ev["aenderungen_html"]

    previous_block = ""
    if ev.get("bisheriger_html"):
        previous_block = "<h3>Bisheriger Inhalt</h3>" + ev["bisheriger_html"]

    return header + "<hr/>" + changes_block + "<hr/>" + previous_block



# --- Processing (ohne DB, mit state.json) ---
@dataclasses.dataclass
class SiteCfg:
    name: str
    bundesland: str
    url: str
    selectors: List[str]
    mode: str = "text"  # or "html"

async def process_site(state: Dict[str, Any], client: httpx.AsyncClient, cfg: SiteCfg, timeout: int) -> Optional[Dict[str, Any]]:
    html_text = await fetch(client, cfg.url, timeout)
    if not html_text:
        return None

    display_text, meta = extract(html_text, cfg.selectors, cfg.mode, site_name=cfg.name, site_url=cfg.url)

    # Hash nur auf normalisiertem Text
    h = make_hash(meta["hash_text"])
    slug = slugify(cfg.name)

    site_state = state["sites"].get(slug, {})
    last_hash = site_state.get("hash")
    last_full = site_state.get("full_text", "")  # bisheriger Volltext (mit Absätzen)

    if h == last_hash:
        return None  # keine Änderung

    # Neue Absätze extrahieren (statt Wort-Diff)
    added_html = added_paragraphs_html(last_full or "", display_text)
    excerpt = meta["display_text"][:2000]  # längerer Auszug ok
    now_iso = now_utc().isoformat()

    # State aktualisieren (inkl. Volltext zur nächsten Diff-Basis)
    state["sites"][slug] = {
        "name": cfg.name,
        "bundesland": cfg.bundesland,
        "url": cfg.url,
        "hash": h,
        "current_html": meta["display_text"],  # voller aktueller HTML-Inhalt
        "previous_html": site_state.get("current_html", ""),  # alter Stand speichern
        "last_change": now_iso,
    }

    if not last_hash:
        # Erste Erfassung
        state["items"].append({
            "slug": slug,
            "name": cfg.name,
            "bundesland": cfg.bundesland,
            "url": cfg.url,
            "fetched_at": now_iso,
            "checked_at": meta["checked_at"],
            "selectors": meta["selectors"],
            "selectors_used": meta["selectors_used"],
            "used_nodes": meta["used_nodes"],
            "aenderungen_html": "",  # keine Änderungen
            "bisheriger_html": display_text,  # kompletter Stand
        })
    else:
        # Änderung
        added_html = added_paragraphs_html(last_full or "", display_text)
        state["items"].append({
            "slug": slug,
            "name": cfg.name,
            "bundesland": cfg.bundesland,
            "url": cfg.url,
            "fetched_at": now_iso,
            "checked_at": meta["checked_at"],
            "selectors": meta["selectors"],
            "selectors_used": meta["selectors_used"],
            "used_nodes": meta["used_nodes"],
            "aenderungen_html": added_html,  # nur die neuen Absätze
            "bisheriger_html": site_state.get("current_html", ""),  # letzter Stand
        })

    if len(state["items"]) > 2000:
        state["items"] = state["items"][-2000:]

    return {
        "site": cfg,
        "fetched_at": now_iso,
        "hash": h,
        "excerpt": excerpt,
        "diff_html": "",  # nicht mehr genutzt
    }


def rfc2822(ts_iso: str) -> str:
    dt_obj = dt.datetime.fromisoformat(ts_iso)
    from email.utils import format_datetime
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return format_datetime(dt_obj)

def generate_feeds_from_state(state: Dict[str, Any], feeds_path: str, retention_days: int, active_slugs: List[str]):
    ensure_dir(feeds_path)
    if not active_slugs:
        return

    cutoff_dt = now_utc() - dt.timedelta(days=retention_days)
    build_ts_rfc2822 = rfc2822(now_utc().isoformat())

    # --- Per-Site-Feeds nur für aktive Slugs
    items_by_slug: Dict[str, List[Dict[str, Any]]] = {}
    for ev in state.get("items", []):
        if ev["slug"] in active_slugs:
            ev_dt = dt.datetime.fromisoformat(ev["fetched_at"])
            if ev_dt >= cutoff_dt:
                items_by_slug.setdefault(ev["slug"], []).append(ev)

    for slug, evs in items_by_slug.items():
        evs.sort(key=lambda e: e["fetched_at"], reverse=True)
        meta = state["sites"].get(slug, {})
        name = meta.get("name", slug)
        url = meta.get("url", "")
        rss_items = []
        for ev in evs:
            fetched_at = ev["fetched_at"]
            rss_items.append({
                "title": f"Aktualisierung: {name} ({fetched_at[:19]}Z)",
                "link": url,
                "guid": f"{slug}:{fetched_at}",
                "pubDate": rfc2822(fetched_at),
                "description": build_item_description(ev),
            })
        xml = make_rss(
            channel_title=f"Aktualisierungen – {name}",
            channel_link=url,
            channel_desc=f"Änderungsfeed für {name}",
            items=rss_items,
            last_build_date=build_ts_rfc2822,
        )
        write_text(os.path.join(feeds_path, f"site_{slug}.xml"), xml)

    # --- Aggregation pro Bundesland (nur aktive Slugs)
    ev_all: List[Dict[str, Any]] = []
    for ev in state.get("items", []):
        if ev["slug"] in active_slugs:
            ev_dt = dt.datetime.fromisoformat(ev["fetched_at"])
            if ev_dt >= cutoff_dt:
                ev_all.append(ev)
    ev_all.sort(key=lambda e: e["fetched_at"], reverse=True)

    by_bl: Dict[str, List[Dict[str, Any]]] = {}
    for ev in ev_all:
        by_bl.setdefault(ev["bundesland"], []).append(ev)

    for bl, evs in by_bl.items():
        rss_items = []
        for ev in evs:
            fetched_at = ev["fetched_at"]
            rss_items.append({
                "title": f"{ev['name']} – Update {fetched_at[:19]}Z",
                "link": ev["url"],
                "guid": f"{ev['slug']}:{fetched_at}",
                "pubDate": rfc2822(fetched_at),
                "description": build_item_description(ev),
            })
        xml = make_rss(
            channel_title=f"Regional-/Entwicklungspläne – {bl}",
            channel_link="https://example.invalid/",
            channel_desc=f"Aggregierter Feed für {bl}",
            items=rss_items,
            last_build_date=build_ts_rfc2822,
        )
        bl_slug = slugify(bl)
        write_text(os.path.join(feeds_path, f"DE-{bl_slug}.xml"), xml)

# --- Main ---
async def main(args):
    cfg = load_config("config.yml")

    storage_path = cfg.get("storage_path", DEFAULT_STORAGE)
    feeds_path = cfg.get("feeds_path", DEFAULT_FEEDS)
    ensure_dir("logs")
    ensure_dir(storage_path)
    ensure_dir(feeds_path)

    state = load_state(storage_path)

    headers = {"User-Agent": cfg.get("user_agent", "DE-Plan-Feed-Watcher/1.0")}
    timeout = int(cfg.get("site_timeout_sec", 30))
    async with httpx.AsyncClient(headers=headers) as client:
        tasks = []
        for s in cfg["sites"]:
            scfg = SiteCfg(name=s["name"], bundesland=s["bundesland"], url=s["url"],
                           selectors=s.get("selectors", []), mode=s.get("mode", "text"))
            tasks.append(process_site(state, client, scfg, timeout))
        results = await asyncio.gather(*tasks, return_exceptions=True)
        changed = [r for r in results if isinstance(r, dict)]
        print(f"Checked {len(tasks)} sites – changes: {len(changed)}")

    # Speichern
    save_state(storage_path, state)

    # Feeds erzeugen (nur aktive Slugs aus aktueller Config)
    active_slugs = [slugify(s["name"]) for s in cfg["sites"]]
    generate_feeds_from_state(state, feeds_path, int(cfg.get("feed_retention_days", 120)), active_slugs)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Einmalig ausführen und beenden")
    args = parser.parse_args()
    asyncio.run(main(args))
