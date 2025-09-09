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

# ======================================================================================================================
### Input variables

DEFAULT_STORAGE = "./data"
DEFAULT_FEEDS = "./feeds"
STATE_FILENAME = "state.json"  # wird unter storage_path abgelegt


# ======================================================================================================================
### Helper functions

def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return re.sub(r"-+", "-", value)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ensure_dir(p: str):
    """Verbesserte Verzeichnis-Erstellung"""
    if p:  # Nur wenn Pfad nicht leer
        try:
            os.makedirs(p, exist_ok=True)
        except Exception as e:
            print(f"WARNING: Could not create directory {p}: {e}")


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
    # Nur Whitespace normalisieren
    text = re.sub(r"\s+", " ", text).strip()

    # NUR rein technische IDs entfernen
    text = re.sub(r'\b[a-f0-9]{32,}\b', '[TECH_ID]', text, flags=re.IGNORECASE)
    text = re.sub(r'[?&]sessionid=[a-f0-9]{20,}', '', text, flags=re.IGNORECASE)

    # Zeitstempel NICHT entfernen, außer es sind eindeutig technische
    text = re.sub(r'(Seitenaufruf um \d{2}:\d{2}:\d{2})', '[PAGELOAD]', text)

    return text

"""
# Erweiterte Normalisierung von Hash um technische Änderungen die eine Referenz im Selektorblock haben auszuschließen 
### weniger falls errors?? 
def normalize_for_hash(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    
    # ASP.NET spezifische Artefakte
    text = re.sub(r'DependencyHandler\.ashx\?[^"\'>\s]*', '[DEPENDENCY_HANDLER]', text)
    text = re.sub(r'WebResource\.ashx\?[^"\'>\s]*', '[WEB_RESOURCE]', text)
    text = re.sub(r'ScriptResource\.ashx\?[^"\'>\s]*', '[SCRIPT_RESOURCE]', text)
    
    # Cache-Buster und Versionshashes
    text = re.sub(r'[?&]v=[a-f0-9]{8,}', '', text, flags=re.IGNORECASE)
    text = re.sub(r'[?&]t=\d{10,}', '', text)  # Unix timestamps
    
    # Bestehende Regeln...
    text = re.sub(r'\b[a-f0-9]{32,}\b', '[TECH_ID]', text, flags=re.IGNORECASE)
    text = re.sub(r'[?&]sessionid=[a-f0-9]{20,}', '', text, flags=re.IGNORECASE)
    
    return text
"""


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


def added_paragraphs_html(old_text: str, new_text: str, site_name: str = "") -> str:
    """Verbesserte Diff-Erkennung für Absätze - einheitlich für alle Sites"""
    import difflib

    if not old_text.strip():
        # Erste Erfassung: keine "Änderungen" anzeigen
        return "<p><em>Erste Erfassung - keine Änderungen zu vergleichen.</em></p>"

    # --- NEU: Eingaben vereinheitlichen -> Plaintext (HTML-Tags entfernen)
    def _to_plain(s: str) -> str:
        if not s:
            return ""
        if "<" in s and ">" in s:
            try:
                from bs4 import BeautifulSoup
                return BeautifulSoup(s, "lxml").get_text(separator="\n", strip=True)
            except Exception:
                pass
        return s

    old_plain = _to_plain(old_text)
    new_plain = _to_plain(new_text)

    # Ab hier wie gehabt – aber mit Plaintext weiterarbeiten
    old_pars = split_paragraphs(old_plain)
    new_pars = split_paragraphs(new_plain)

    # Einheitliche minimale Normalisierung für alle Sites
    old_pars_norm = [normalize_for_hash(p) for p in old_pars]
    new_pars_norm = [normalize_for_hash(p) for p in new_pars]

    sm = difflib.SequenceMatcher(None, old_pars_norm, new_pars_norm)
    added: list[str] = []

    for tag, i1, i2, j1, j2 in sm.get_opcodes():
        if tag == "insert":
            added.extend(new_pars[j1:j2])  # Original-Absätze (Plaintext) verwenden
        elif tag == "replace":
            for old_idx, new_idx in zip(range(i1, i2), range(j1, j2)):
                if old_idx < len(old_pars_norm) and new_idx < len(new_pars_norm):
                    old_clean = re.sub(r'\[TECH_ID\]|\[SESSION\]|\[SEITENAUFRUF\]|\[GENERIERUNG\]', '', old_pars_norm[old_idx])
                    new_clean = re.sub(r'\[TECH_ID\]|\[SESSION\]|\[SEITENAUFRUF\]|\[GENERIERUNG\]', '', new_pars_norm[new_idx])
                    if old_clean.strip() != new_clean.strip():
                        added.append(new_pars[new_idx])

    if not added:
        return "<p><em>Keine neuen oder substantiell geänderten Inhalte erkannt.</em></p>"

    return paragraphs_to_html(added)  # <p>…</p> mit escapetem Text


def rfc2822(ts_iso: str) -> str:
    dt_obj = dt.datetime.fromisoformat(ts_iso)
    from email.utils import format_datetime
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return format_datetime(dt_obj)


def _safe_rfc2822(ts: Optional[str]) -> str:
    if not ts:
        return ""
    try:
        return rfc2822(ts)
    except Exception:
        return ts  # Fallback, um nicht hart zu crashen


def _event_ts(ev: Dict[str, Any]) -> Optional[str]:
    return ev.get("detected_at") or ev.get("checked_at") or ev.get("first_seen") or ev.get("fetched_at")

# ======================================================================================================================

def rss_escape(s: str) -> str:
    return html.escape(s, quote=True)


def make_rss(channel_title: str, channel_link: str, channel_desc: str, items: List[Dict[str, str]], *,
             last_build_date: Optional[str] = None) -> str:
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
    if not html_payload:
        return "<![CDATA[]]>"

    # XML-ungültige Zeichen entfernen
    html_payload = xml_sanitize(html_payload)

    # Existierende CDATA-Sequenzen escapen
    html_payload = html_payload.replace("]]>", "]]]]><![CDATA[>")

    return f"<![CDATA[{html_payload}]]>"


# ======================================================================================================================
### Configuration of yml for scraping logic

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


# ======================================================================================================================
### Storage of websites state

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
        "items": []  # Historie der Änderungen (Events)
    }


def save_state(storage_path: str, state: Dict[str, Any]):
    path = state_path(storage_path)
    ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ======================================================================================================================
### OOP with class structured data storage for fetching websites

@dataclasses.dataclass
class SiteCfg:
    name: str
    bundesland: str
    url: str
    selectors: List[str]
    mode: str = "text"  # or "html"


async def fetch(client: httpx.AsyncClient, url: str, timeout: int) -> Optional[str]:
    try:
        r = await client.get(url, timeout=timeout, follow_redirects=True)
        r.raise_for_status()
        r.encoding = r.encoding or "utf-8"
        return r.text
    except Exception as e:
        print(f"FETCH ERROR for {url}: {e}")
        return None


# ======================================================================================================================
### Analyse textual difference from website <-> state.json last screening

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


# ======================================================================================================================
### Extract textual html information from website and log process steps

def extract(html_text: str, selectors: List[str], mode: str, *, site_name: str = "", site_url: str = "") -> tuple[
    str, Dict[str, Any]]:
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
        try:
            for node in soup.select(sel):
                matches.append(node)
        except Exception as e:
            print(f"CSS selector error '{sel}' for {site_name}: {e}")

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
        # Für Hash: Plaintext + minimale Normalisierung für alle Sites
        plaintext = node.get_text(separator=" ", strip=True)
        hash_chunks.append(normalize_for_hash(plaintext))

    display_text = "\n\n".join(display_chunks).strip()
    hash_text = " ".join(hash_chunks)

    ts = now_utc().isoformat()
    node_labels = ", ".join(node_label(n) for n in used_nodes[:3])
    if len(used_nodes) > 3:
        node_labels += f" (+{len(used_nodes) - 3} more)"
    selectors_pretty = "[" + ", ".join(sel_list) + "]" if sel_list else "[]"

    log_block = (
        f"[{ts}] site={site_name}\n"
        f"  url={site_url}\n"
        f"  strategy={used_strategy}\n"
        f"  selectors={selectors_pretty}\n"
        f"  matches={len(matches)}\n"
        f"  used_nodes={node_labels}\n"
        f"  text_len={len(display_text)} hash={short_hash(hash_text)}\n"
        f"  hash_text_preview={hash_text[:100]}...\n"
    )
    append_log(log_block)

    meta = {
        "checked_at": ts,
        "strategy": used_strategy,
        "selectors": sel_list,
        "selectors_used": sel_list if matches else [used_strategy.replace("fallback:", "(fallback: ") + ")"],
        "used_nodes": node_labels,
        "display_text": display_text,  # für Darstellung/Absätze
        "hash_text": hash_text,  # für Hash/Abgleich
    }
    # return: (anzeige-text, meta) – der anzeige-text ist mit absätzen
    return display_text, meta


# ======================================================================================================================
### Build elements of RSS Article

def build_item_description(ev: Dict[str, Any]) -> str:
    def _fmt(ts: Optional[str]) -> str:
        if not ts:
            return ""
        try:
            return rfc2822(ts)
        except Exception:
            return ts  # Fallback: ungeparst anzeigen

    detected_at = ev.get("detected_at")
    checked_at  = ev.get("checked_at")
    first_seen  = ev.get("first_seen")  # seit wann überwacht

    selectors_used = ev.get("selectors_used") or ev.get("selectors") or []
    selectors_txt  = ", ".join(selectors_used) if selectors_used else "–"
    used_nodes     = ev.get("used_nodes", "")

    header = (
        f"<p><strong>Erkannt (Änderung):</strong> {rss_escape(_fmt(detected_at))}<br>"
        f"<strong>Zuletzt geprüft:</strong> {rss_escape(_fmt(checked_at))}<br>"
        f"<strong>Seit Monitoring:</strong> {rss_escape(_fmt(first_seen))}<br>"
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


# ======================================================================================================================
### Single website procesing

async def process_site(state: Dict[str, Any], client: httpx.AsyncClient, cfg: SiteCfg, timeout: int) -> Optional[
    Dict[str, Any]]:
    slug = slugify(cfg.name)
    now_iso = now_utc().isoformat()

    # Basis-State für diese Site sicherstellen
    if slug not in state["sites"]:
        state["sites"][slug] = {
            "name": cfg.name,
            "bundesland": cfg.bundesland,
            "url": cfg.url,
            "hash": None,
            "current_content": None,
            "first_seen": now_iso,
            "detected_at": now_iso,
            "last_checked": now_iso,
            "last_change": None,
            "consecutive_errors": 0,
            "last_error": None,
            "last_error_time": None
        }

    site_state = state["sites"][slug]

    try:
        # Fetch mit Fehlerbehandlung
        html_text = await fetch(client, cfg.url, timeout)

        if not html_text:
            # Fetch fehlgeschlagen - Error tracking
            site_state["consecutive_errors"] = site_state.get("consecutive_errors", 0) + 1
            site_state["last_error"] = "Fetch returned empty content"
            site_state["last_error_time"] = now_iso
            site_state["last_checked"] = now_iso

            print(f"WARNING {cfg.name}: Fetch failed (attempt #{site_state['consecutive_errors']})")

            # Nach 3 Fehlversuchen einen Fehler-Item erstellen
            if site_state["consecutive_errors"] == 3:
                state["items"].append({
                    "slug": slug,
                    "name": cfg.name,
                    "bundesland": cfg.bundesland,
                    "url": cfg.url,
                    "first_seen": now_iso,
                    "detected_at": now_iso,
                    "checked_at": now_iso,
                    "selectors": cfg.selectors,
                    "selectors_used": [],
                    "used_nodes": "",
                    "aenderungen_html": f"<p><strong>Website nicht erreichbar</strong><br>Die Seite konnte 3x in Folge nicht abgerufen werden.</p>",
                    "bisheriger_html": "",
                })

            return None

        # Erfolgreich gefetcht - Error counter zurücksetzen
        if site_state.get("consecutive_errors", 0) > 0:
            print(f"INFO {cfg.name}: Site wieder erreichbar nach {site_state['consecutive_errors']} Fehlversuchen")
            site_state["consecutive_errors"] = 0
            site_state["last_error"] = None
            site_state["last_error_time"] = None

        # Content extraction mit Fehlerbehandlung
        try:
            display_text, meta = extract(html_text, cfg.selectors, cfg.mode,
                                         site_name=cfg.name, site_url=cfg.url)
        except Exception as e:
            print(f"ERROR {cfg.name}: Extraction failed: {e}")
            site_state["last_error"] = f"Extraction error: {str(e)[:200]}"
            site_state["last_error_time"] = now_iso
            site_state["last_checked"] = now_iso
            return None

        # Hash berechnen
        h = make_hash(meta["hash_text"])
        last_hash = site_state.get("hash")

        # DEBUG: Hash-Vergleich
        print(f"DEBUG {cfg.name}: current_hash={h[:12]}, stored_hash={str(last_hash)[:12] if last_hash else 'None'}")

        # Erste Erfassung dieser Site
        if last_hash is None:
            state["sites"][slug].update({
                "hash": h,
                "current_content": display_text,
                # WICHTIG: previous_content NICHT setzen bei erster Erfassung
                "first_seen": now_iso,
                "last_change": now_iso,
                "last_checked": now_iso,
            })

            # Info-Item für erste Erfassung
            state["items"].append({
                "slug": slug,
                "name": cfg.name,
                "bundesland": cfg.bundesland,
                "url": cfg.url,
                "first_seen": now_iso,
                "detected_at": now_iso,
                "checked_at": meta["checked_at"],
                "selectors": meta["selectors"],
                "selectors_used": meta["selectors_used"],
                "used_nodes": meta["used_nodes"],
                "aenderungen_html": "<p><em>Erste Erfassung - Monitoring gestartet.</em></p>",
                "bisheriger_html": f"<div style='max-height:400px;overflow-y:auto'>{display_text}</div>",
            })

            print(f"{cfg.name}: Erste Erfassung erfolgreich")

            return {
                "site": cfg,
                "fetched_at": now_iso,
                "hash": h,
                "excerpt": display_text[:2000],
                "diff_html": "",
                "is_initial": True
            }

        # Update für bestehende Site
        state["sites"][slug]["last_checked"] = now_iso

        # Keine Änderung
        if h == last_hash:
            print(f"{cfg.name}: Keine Änderung")
            return None

        # ÄNDERUNG ERKANNT
        old_content = site_state.get("current_content", "")

        # Sicherheitscheck: Wenn old_content leer aber last_hash existiert -> Dateninkonsistenz
        if not old_content and last_hash:
            print(f"WARNING {cfg.name}: Inkonsistenter State - Hash vorhanden aber kein Content")
            old_content = "<p><em>Vorheriger Inhalt nicht verfügbar (Dateninkonsistenz)</em></p>"

        # Änderungen berechnen
        try:
            added_html = added_paragraphs_html(old_content, display_text, cfg.name)
        except Exception as e:
            print(f"WARNING {cfg.name}: Diff-Berechnung fehlgeschlagen: {e}")
            added_html = "<p><em>Änderungen konnten nicht berechnet werden</em></p>"

        print(f"{cfg.name}: ÄNDERUNG ERKANNT! Hash {str(last_hash)[:12]} -> {h[:12]}"
              f"Änderung besteht aus: {added_html}")

        # State aktualisieren
        state["sites"][slug].update({
            "hash": h,
            "previous_content": old_content,
            "current_content": display_text,
            "last_change": now_iso,
        })

        # RSS-Item für Änderung
        state["items"].append({
            "slug": slug,
            "name": cfg.name,
            "bundesland": cfg.bundesland,
            "url": cfg.url,
            "first_seen": site_state.get("first_seen", now_iso),
            "detected_at": now_iso,
            "checked_at": meta["checked_at"],
            "selectors": meta["selectors"],
            "selectors_used": meta["selectors_used"],
            "used_nodes": meta["used_nodes"],
            "aenderungen_html": added_html,
            "bisheriger_html": f"<div style='max-height:400px;overflow-y:auto'>{old_content}</div>",
        })

        # Items-Liste begrenzen (effizienter mit deque wäre besser)
        if len(state["items"]) > 2000:
            state["items"] = state["items"][-2000:]

        return {
            "site": cfg,
            "fetched_at": now_iso,
            "hash": h,
            "excerpt": display_text[:2000],
            "diff_html": added_html,
            "is_change": True
        }

    except httpx.TimeoutException:
        print(f"ERROR {cfg.name}: Timeout nach {timeout}s")
        site_state["consecutive_errors"] = site_state.get("consecutive_errors", 0) + 1
        site_state["last_error"] = f"Timeout after {timeout}s"
        site_state["last_error_time"] = now_iso
        site_state["last_checked"] = now_iso
        return None

    except httpx.HTTPStatusError as e:
        print(f"ERROR {cfg.name}: HTTP {e.response.status_code}")
        site_state["consecutive_errors"] = site_state.get("consecutive_errors", 0) + 1
        site_state["last_error"] = f"HTTP {e.response.status_code}"
        site_state["last_error_time"] = now_iso
        site_state["last_checked"] = now_iso
        return None

    except Exception as e:
        # Unerwarteter Fehler
        print(f"CRITICAL ERROR {cfg.name}: {type(e).__name__}: {e}")
        site_state["last_error"] = f"Unexpected: {type(e).__name__}"
        site_state["last_error_time"] = now_iso
        site_state["last_checked"] = now_iso

        # Bei kritischen Fehlern trotzdem State speichern
        import traceback
        traceback.print_exc()

        return None


# ======================================================================================================================
### Generate single feeds for each screened website

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
            ts = _event_ts(ev)
            if ts:
                ev_dt = dt.datetime.fromisoformat(ts)
                if ev_dt >= cutoff_dt:
                    items_by_slug.setdefault(ev["slug"], []).append(ev)

    for slug, evs in items_by_slug.items():
        # Sortiere nach Ereigniszeit absteigend
        evs.sort(key=lambda e: _event_ts(e) or "", reverse=True)

        meta = state["sites"].get(slug, {})
        name = meta.get("name", slug)
        url = meta.get("url", "")
        rss_items = []

        for ev in evs:
            ts = _event_ts(ev) or now_utc().isoformat()
            ts_human = ts[:19] + "Z" if "T" in ts else ts
            rss_items.append({
                "title": f"Aktualisierung: {name} ({ts_human})",
                "link": url,
                "guid": f"{slug}:{ts}",  # GUID stabil auf Event-Zeit
                "pubDate": _safe_rfc2822(ts),
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
            ts = _event_ts(ev)
            if ts:
                ev_dt = dt.datetime.fromisoformat(ts)
                if ev_dt >= cutoff_dt:
                    ev_all.append(ev)

    ev_all.sort(key=lambda e: _event_ts(e) or "", reverse=True)

    by_bl: Dict[str, List[Dict[str, Any]]] = {}
    for ev in ev_all:
        by_bl.setdefault(ev["bundesland"], []).append(ev)

    for bl, evs in by_bl.items():
        rss_items = []
        for ev in evs:
            ts = _event_ts(ev) or now_utc().isoformat()
            ts_human = ts[:19] + "Z" if "T" in ts else ts
            rss_items.append({
                "title": f"{ev['name']} – Update {ts_human}",
                "link": ev["url"],
                "guid": f"{ev['slug']}:{ts}",
                "pubDate": _safe_rfc2822(ts),
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
        write_text(os.path.join(feeds_path, f"2_DE-{bl_slug}.xml"), xml)


# ======================================================================================================================
### Main logic query

async def main(args):
    cfg = load_config("config.yml")

    storage_path = cfg.get("storage_path", DEFAULT_STORAGE)
    feeds_path = cfg.get("feeds_path", DEFAULT_FEEDS)

    # Explizite Verzeichnis-Erstellung mit Debugging
    print(f"Creating directories: logs, {storage_path}, {feeds_path}")
    ensure_dir("logs")
    ensure_dir(storage_path)
    ensure_dir(feeds_path)

    state = load_state(storage_path)
    print(f"Loaded state: {len(state.get('sites', {}))} sites, {len(state.get('items', []))} items")

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

    print(f"Generated feeds for {len(active_slugs)} sites")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Einmalig ausführen und beenden")
    args = parser.parse_args()
    asyncio.run(main(args))
