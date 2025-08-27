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
from bs4 import BeautifulSoup

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

# --- RSS minimal (no external deps) ---
def rss_escape(s: str) -> str:
    return html.escape(s, quote=True)

def make_rss(channel_title: str, channel_link: str, channel_desc: str, items: List[Dict[str, str]]) -> str:
    # items: list of dict with title, link, guid, pubDate (RFC2822), description (CDATA)
    rss = ['<?xml version="1.0" encoding="UTF-8"?>',
           '<rss version="2.0">',
           "<channel>",
           f"<title>{rss_escape(channel_title)}</title>",
           f"<link>{rss_escape(channel_link)}</link>",
           f"<description>{rss_escape(channel_desc)}</description>"]
    for it in items:
        desc = it.get("description", "")
        rss.append("<item>")
        rss.append(f"<title>{rss_escape(it.get('title',''))}</title>")
        rss.append(f"<link>{rss_escape(it.get('link',''))}</link>")
        rss.append(f"<guid isPermaLink=\"false\">{rss_escape(it.get('guid',''))}</guid>")
        rss.append(f"<pubDate>{rss_escape(it.get('pubDate',''))}</pubDate>")
        # put description in CDATA to preserve diffs/markup
        rss.append(f"<![CDATA[{desc}]]>")
        rss.append("</item>")
    rss.append("</channel></rss>")
    return "\n".join(rss)

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

# --- Extraction + Logging ---
def extract(html_text: str, selectors: List[str], mode: str, *, site_name: str = "", site_url: str = "") -> str:
    soup = BeautifulSoup(html_text, "lxml")
    sel_list = [s.strip() for s in (selectors or []) if s and s.strip()]

    matches = []
    for sel in sel_list:
        for node in soup.select(sel):
            matches.append(node)

    # Auswahl-Strategie bestimmen
    if matches:
        used_strategy = f"selectors({', '.join(sel_list)})"
        used_nodes = matches
    else:
        # Fallback-Kaskade: main -> body -> soup
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
    chunks = []
    for node in used_nodes:
        if mode == "html":
            chunks.append(str(node))
        else:
            chunks.append(node.get_text(separator="\n", strip=True))
    content = "\n\n".join(chunks).strip()
    content = re.sub(r"\s+", " ", content)  # Whitespace normalisieren

    # Logging (Block)
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
        f"  text_len={len(content)} hash={short_hash(content)}\n"
    )
    append_log(log_block)

    return content

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
    # wrap in <pre>
    return "<pre>" + html.escape("\n".join(lines)) + "</pre>"

# --- Processing (ohne DB, mit state.json) ---
async def process_site(state: Dict[str, Any], client: httpx.AsyncClient, cfg: SiteCfg, timeout: int) -> Optional[Dict[str, Any]]:
    html_text = await fetch(client, cfg.url, timeout)
    if not html_text:
        return None

    content = extract(html_text, cfg.selectors, cfg.mode, site_name=cfg.name, site_url=cfg.url)
    h = make_hash(content)
    slug = slugify(cfg.name)

    site_state = state["sites"].get(slug, {})
    last_hash = site_state.get("hash")
    last_excerpt = site_state.get("excerpt", "")

    if h == last_hash:
        return None  # keine Änderung

    # compute diff if previous exists
    diff_html = text_diff(last_excerpt or "", content) if last_hash else "<p>Erste Erfassung.</p>"
    excerpt = content[:1000]
    now_iso = now_utc().isoformat()

    # State aktualisieren
    state["sites"][slug] = {
        "name": cfg.name,
        "bundesland": cfg.bundesland,
        "url": cfg.url,
        "hash": h,
        "excerpt": excerpt,
        "last_change": now_iso,
    }

    # Historie-Event anhängen (für Feeds/Aggregation)
    state["items"].append({
        "slug": slug,
        "name": cfg.name,
        "bundesland": cfg.bundesland,
        "url": cfg.url,
        "fetched_at": now_iso,
        "diff_html": diff_html,
    })
    # Historie begrenzen (z. B. auf 2000 Events)
    if len(state["items"]) > 2000:
        state["items"] = state["items"][-2000:]

    return {
        "site": cfg,
        "fetched_at": now_iso,
        "hash": h,
        "excerpt": excerpt,
        "diff_html": diff_html,
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
                "description": ev["diff_html"]
            })
        xml = make_rss(
            channel_title=f"Aktualisierungen – {name}",
            channel_link=url,
            channel_desc=f"Änderungsfeed für {name}",
            items=rss_items
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
                "description": ev["diff_html"]
            })
        xml = make_rss(
            channel_title=f"Regional-/Entwicklungspläne – {bl}",
            channel_link="https://example.invalid/",
            channel_desc=f"Aggregierter Feed für {bl}",
            items=rss_items
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
