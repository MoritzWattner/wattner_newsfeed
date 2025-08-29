#!/usr/bin/env python3
"""
DE-Plan Feed Watcher - Optimierte Website Change Detection
"""

import argparse
import asyncio
import dataclasses
import datetime as dt
import hashlib
import html
import json
import os
import re
from typing import Dict, List, Optional, Any

import httpx
import yaml
from bs4 import BeautifulSoup, Comment

# ======================================================================================================================
# Configuration
# ======================================================================================================================

DEFAULT_STORAGE = "./data"
DEFAULT_FEEDS = "./feeds"
STATE_FILENAME = "state.json"


@dataclasses.dataclass
class SiteConfig:
    name: str
    bundesland: str
    url: str
    selectors: List[str]
    mode: str = "text"
    update_frequency: str = "normal"  # fast, normal, slow
    priority: str = "normal"  # high, normal, low


# ======================================================================================================================
# Utilities
# ======================================================================================================================

def slugify(value: str) -> str:
    """Convert string to URL-safe slug"""
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value).strip("-")
    return re.sub(r"-+", "-", value)


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def ensure_dir(path: str) -> bool:
    """Create directory if it doesn't exist. Return success status."""
    if not path:
        return False
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception as e:
        print(f"ERROR: Could not create directory {path}: {e}")
        return False


def make_hash(text: str) -> str:
    """Generate SHA256 hash of text"""
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def short_hash(hash_str: str) -> str:
    """Return first 12 chars of hash for display"""
    return hash_str[:12] if hash_str else "None"


# ======================================================================================================================
# Content Processing
# ======================================================================================================================

def normalize_content(text: str) -> str:
    """MINIMAL normalization - only remove technical artifacts"""
    if not text:
        return ""

    # Basic whitespace normalization
    text = re.sub(r"\s+", " ", text).strip()

    # Remove only clearly technical artifacts
    text = re.sub(r'\b[a-f0-9]{32,}\b', '[ID]', text, flags=re.IGNORECASE)  # Long hex IDs
    text = re.sub(r'\bsessionid=[a-f0-9]{16,}', 'sessionid=[SESSION]', text, flags=re.IGNORECASE)
    text = re.sub(r'\bjsessionid=[a-f0-9]{16,}', 'jsessionid=[SESSION]', text, flags=re.IGNORECASE)

    # Only very specific technical timestamps
    text = re.sub(r'generiert am \d{2}\.\d{2}\.\d{4} um \d{2}:\d{2}:\d{2}', '[GENERATED]', text, flags=re.IGNORECASE)
    text = re.sub(r'seitenaufruf um \d{2}:\d{2}:\d{2}', '[PAGE_LOAD]', text, flags=re.IGNORECASE)

    return text


def extract_content(html_text: str, selectors: List[str], site_name: str = "") -> tuple[str, Dict[str, Any]]:
    """Extract content from HTML using selectors with fallback strategy"""

    soup = BeautifulSoup(html_text, "lxml")

    # Remove unwanted elements
    for tag in soup(["script", "style", "noscript", "iframe", "template"]):
        tag.decompose()
    for comment in soup.find_all(string=lambda x: isinstance(x, Comment)):
        comment.extract()

    # Try selectors with fallback chain
    selected_nodes = []
    used_selectors = []

    for selector in selectors:
        if selector.strip():
            try:
                nodes = soup.select(selector.strip())
                if nodes:
                    selected_nodes.extend(nodes)
                    used_selectors.append(selector.strip())
            except Exception as e:
                print(f"WARNING: Invalid selector '{selector}' for {site_name}: {e}")

    # Fallback strategy if no selectors worked
    if not selected_nodes:
        fallbacks = ["main", ".main-content", ".content", "#content", "#main"]
        for fallback in fallbacks:
            nodes = soup.select(fallback)
            if nodes:
                selected_nodes = nodes
                used_selectors = [f"fallback:{fallback}"]
                break

        # Ultimate fallback
        if not selected_nodes and soup.body:
            selected_nodes = [soup.body]
            used_selectors = ["fallback:body"]

    # Extract text and HTML
    if not selected_nodes:
        return "", {"error": "No content found", "selectors_used": []}

    content_html = ""
    content_text = ""

    for node in selected_nodes:
        content_html += str(node) + "\n\n"
        content_text += node.get_text(separator=" ", strip=True) + " "

    # Normalize for hash comparison
    normalized_text = normalize_content(content_text.strip())

    return content_html.strip(), {
        "content_text": content_text.strip(),
        "normalized_text": normalized_text,
        "selectors_used": used_selectors,
        "node_count": len(selected_nodes),
        "content_length": len(content_text),
    }


def calculate_content_diff(old_text: str, new_text: str) -> str:
    """Calculate meaningful differences between old and new content"""

    if not old_text.strip():
        return "<p><em>Erste Erfassung - Überwachung gestartet.</em></p>"

    # Split into paragraphs for comparison
    def split_paragraphs(text: str) -> List[str]:
        text = text.replace('\r\n', '\n').replace('\r', '\n')
        text = re.sub(r'\n{3,}', '\n\n', text)
        return [p.strip() for p in text.split('\n\n') if p.strip()]

    old_paragraphs = split_paragraphs(old_text)
    new_paragraphs = split_paragraphs(new_text)

    # Simple diff - find new paragraphs
    old_set = set(normalize_content(p) for p in old_paragraphs)
    new_content = []

    for p in new_paragraphs:
        normalized_p = normalize_content(p)
        if normalized_p not in old_set and len(normalized_p) > 10:  # Ignore very short changes
            new_content.append(p)

    if not new_content:
        return "<p><em>Keine neuen Inhalte erkannt (möglicherweise nur Formatierungsänderungen).</em></p>"

    # Format as HTML
    html_parts = []
    for content in new_content[:5]:  # Limit to 5 changes
        html_parts.append(f"<p>{html.escape(content)}</p>")

    if len(new_content) > 5:
        html_parts.append(f"<p><em>... und {len(new_content) - 5} weitere Änderungen</em></p>")

    return "".join(html_parts)


# ======================================================================================================================
# State Management
# ======================================================================================================================

def load_state(storage_path: str) -> Dict[str, Any]:
    """Load state from JSON file"""
    state_file = os.path.join(storage_path, STATE_FILENAME)

    if not os.path.exists(state_file):
        return {"sites": {}, "items": [], "metadata": {"version": "2.0", "created": now_utc().isoformat()}}

    try:
        with open(state_file, 'r', encoding='utf-8') as f:
            state = json.load(f)
            # Ensure metadata exists
            if "metadata" not in state:
                state["metadata"] = {"version": "2.0", "migrated": now_utc().isoformat()}
            return state
    except Exception as e:
        print(f"ERROR: Could not load state file: {e}")
        return {"sites": {}, "items": [], "metadata": {"version": "2.0", "error_recovery": now_utc().isoformat()}}


def save_state(storage_path: str, state: Dict[str, Any]) -> bool:
    """Save state to JSON file with atomic write"""
    state_file = os.path.join(storage_path, STATE_FILENAME)
    temp_file = state_file + ".tmp"

    try:
        state["metadata"]["last_save"] = now_utc().isoformat()

        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

        os.replace(temp_file, state_file)
        return True
    except Exception as e:
        print(f"ERROR: Could not save state: {e}")
        if os.path.exists(temp_file):
            os.remove(temp_file)
        return False


# ======================================================================================================================
# Website Fetching
# ======================================================================================================================

async def fetch_website(client: httpx.AsyncClient, url: str, timeout: int = 30) -> Optional[str]:
    """Fetch website content with error handling"""
    try:
        response = await client.get(url, timeout=timeout, follow_redirects=True)
        response.raise_for_status()
        response.encoding = response.encoding or "utf-8"
        return response.text
    except httpx.TimeoutException:
        print(f"TIMEOUT: {url}")
    except httpx.HTTPStatusError as e:
        print(f"HTTP {e.response.status_code}: {url}")
    except Exception as e:
        print(f"FETCH ERROR {url}: {e}")
    return None


# ======================================================================================================================
# Site Processing
# ======================================================================================================================

async def process_site(state: Dict[str, Any], client: httpx.AsyncClient, site: SiteConfig) -> Optional[Dict[str, Any]]:
    """Process a single website and detect changes"""

    # Fetch website content
    html_content = await fetch_website(client, site.url)
    if not html_content:
        return None

    # Extract content using selectors
    content_html, meta = extract_content(html_content, site.selectors, site.name)
    if meta.get("error"):
        print(f"EXTRACTION ERROR {site.name}: {meta['error']}")
        return None

    # Calculate content hash
    content_hash = make_hash(meta["normalized_text"])
    slug = slugify(site.name)
    now_iso = now_utc().isoformat()

    # Get current site state
    site_state = state["sites"].get(slug, {})
    last_hash = site_state.get("hash")

    # Always update last_checked
    if slug not in state["sites"]:
        # First time seeing this site
        state["sites"][slug] = {
            "name": site.name,
            "bundesland": site.bundesland,
            "url": site.url,
            "hash": content_hash,
            "first_seen": now_iso,
            "last_checked": now_iso,
            "last_change": now_iso,
            "current_content": content_html,
            "content_length": meta["content_length"],
            "selectors_used": meta["selectors_used"],
        }

        # Add initial item
        state["items"].append({
            "id": f"{slug}:{now_iso}",
            "slug": slug,
            "name": site.name,
            "bundesland": site.bundesland,
            "url": site.url,
            "timestamp": now_iso,
            "change_type": "initial",
            "changes_html": "<p><em>Erste Erfassung - Überwachung gestartet.</em></p>",
            "content_preview": content_html[:500] + "..." if len(content_html) > 500 else content_html,
            "selectors_used": meta["selectors_used"],
        })

        print(f"NEW: {site.name}")
        return {"type": "initial", "site": site, "hash": content_hash}

    else:
        # Update existing site
        state["sites"][slug]["last_checked"] = now_iso

        if content_hash == last_hash:
            # No change detected
            return None

        # Change detected!
        old_content = site_state.get("current_content", "")
        changes_html = calculate_content_diff(old_content, content_html)

        # Update site state
        state["sites"][slug].update({
            "hash": content_hash,
            "last_change": now_iso,
            "previous_content": old_content,
            "current_content": content_html,
            "content_length": meta["content_length"],
            "selectors_used": meta["selectors_used"],
        })

        # Add change item
        state["items"].append({
            "id": f"{slug}:{now_iso}",
            "slug": slug,
            "name": site.name,
            "bundesland": site.bundesland,
            "url": site.url,
            "timestamp": now_iso,
            "change_type": "update",
            "changes_html": changes_html,
            "content_preview": content_html[:500] + "..." if len(content_html) > 500 else content_html,
            "selectors_used": meta["selectors_used"],
        })

        # Cleanup old items (keep last 1000)
        if len(state["items"]) > 1000:
            state["items"] = state["items"][-1000:]

        print(f"CHANGE: {site.name} (hash: {short_hash(last_hash)} -> {short_hash(content_hash)})")
        return {"type": "change", "site": site, "hash": content_hash, "changes": changes_html}


# ======================================================================================================================
# RSS Feed Generation
# ======================================================================================================================

def escape_xml(text: str) -> str:
    """Escape text for XML"""
    return html.escape(text, quote=True)


def wrap_cdata(content: str) -> str:
    """Wrap content in CDATA section"""
    if not content:
        return "<![CDATA[]]>"

    # Escape ]]> sequences
    content = content.replace("]]>", "]]]]><![CDATA[>")
    return f"<![CDATA[{content}]]>"


def format_rfc2822(iso_timestamp: str) -> str:
    """Convert ISO timestamp to RFC2822 format"""
    try:
        dt_obj = dt.datetime.fromisoformat(iso_timestamp)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
        from email.utils import format_datetime
        return format_datetime(dt_obj)
    except Exception:
        return iso_timestamp


def build_rss_description(item: Dict[str, Any], site_info: Dict[str, Any]) -> str:
    """Build RSS item description"""

    timestamp = item.get("timestamp", "")
    first_seen = site_info.get("first_seen", timestamp)
    selectors = ", ".join(item.get("selectors_used", []))

    header = f"""
    <p>
        <strong>Erste Erfassung:</strong> {escape_xml(format_rfc2822(first_seen))}<br>
        <strong>Letzte Änderung:</strong> {escape_xml(format_rfc2822(timestamp))}<br>
        <strong>Selektoren:</strong> {escape_xml(selectors)}<br>
    </p>
    <hr>
    """

    changes_section = ""
    if item.get("changes_html"):
        changes_section = f"<h3>Erkannte Änderungen</h3>{item['changes_html']}<hr>"

    preview_section = f"<h3>Content-Vorschau</h3>{item.get('content_preview', '')}"

    return header + changes_section + preview_section


def generate_site_feed(slug: str, site_info: Dict[str, Any], items: List[Dict[str, Any]]) -> str:
    """Generate RSS feed for a single site"""

    name = site_info.get("name", slug)
    url = site_info.get("url", "")

    # Sort items by timestamp (newest first)
    sorted_items = sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True)

    # Build RSS items
    rss_items = []
    for item in sorted_items[:20]:  # Limit to 20 items
        timestamp = item.get("timestamp", "")
        rss_items.append(f"""
        <item>
            <title>{escape_xml(f"{name} - Aktualisierung")}</title>
            <link>{escape_xml(url)}</link>
            <guid isPermaLink="false">{escape_xml(item.get("id", f"{slug}:{timestamp}"))}</guid>
            <pubDate>{escape_xml(format_rfc2822(timestamp))}</pubDate>
            <description>{wrap_cdata(build_rss_description(item, site_info))}</description>
        </item>
        """)

    # Build complete RSS
    now_rfc = format_rfc2822(now_utc().isoformat())

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
        <channel>
            <title>{escape_xml(f"Änderungen: {name}")}</title>
            <link>{escape_xml(url)}</link>
            <description>{escape_xml(f"Änderungsfeed für {name}")}</description>
            <lastBuildDate>{escape_xml(now_rfc)}</lastBuildDate>
            <language>de-DE</language>
            {"".join(rss_items)}
        </channel>
    </rss>"""

    return rss


def generate_bundesland_feed(bundesland: str, items: List[Dict[str, Any]],
                             site_infos: Dict[str, Dict[str, Any]]) -> str:
    """Generate aggregated RSS feed for a Bundesland"""

    # Sort items by timestamp (newest first)
    sorted_items = sorted(items, key=lambda x: x.get("timestamp", ""), reverse=True)

    # Build RSS items
    rss_items = []
    for item in sorted_items[:50]:  # Limit to 50 items
        site_info = site_infos.get(item.get("slug", ""), {})
        timestamp = item.get("timestamp", "")

        rss_items.append(f"""
        <item>
            <title>{escape_xml(f"{item.get('name', 'Unbekannt')} - Update")}</title>
            <link>{escape_xml(item.get('url', ''))}</link>
            <guid isPermaLink="false">{escape_xml(item.get("id", f"item:{timestamp}"))}</guid>
            <pubDate>{escape_xml(format_rfc2822(timestamp))}</pubDate>
            <description>{wrap_cdata(build_rss_description(item, site_info))}</description>
        </item>
        """)

    # Build complete RSS
    now_rfc = format_rfc2822(now_utc().isoformat())

    rss = f"""<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
        <channel>
            <title>{escape_xml(f"Regionalplanung {bundesland}")}</title>
            <link>https://example.invalid/</link>
            <description>{escape_xml(f"Aggregierte Änderungen für {bundesland}")}</description>
            <lastBuildDate>{escape_xml(now_rfc)}</lastBuildDate>
            <language>de-DE</language>
            {"".join(rss_items)}
        </channel>
    </rss>"""

    return rss


def generate_all_feeds(state: Dict[str, Any], feeds_path: str, active_slugs: List[str]) -> bool:
    """Generate all RSS feeds"""

    if not ensure_dir(feeds_path):
        return False

    sites = state.get("sites", {})
    items = state.get("items", [])

    # Filter items to only active sites and recent items (last 120 days)
    cutoff_date = now_utc() - dt.timedelta(days=120)
    active_items = []

    for item in items:
        if item.get("slug") in active_slugs:
            try:
                item_date = dt.datetime.fromisoformat(item.get("timestamp", ""))
                if item_date >= cutoff_date:
                    active_items.append(item)
            except Exception:
                continue

    # Generate per-site feeds
    items_by_slug: Dict[str, List[Dict[str, Any]]] = {}
    for item in active_items:
        slug = item.get("slug")
        if slug:
            items_by_slug.setdefault(slug, []).append(item)

    site_feeds_generated = 0
    for slug, site_items in items_by_slug.items():
        if slug in sites:
            try:
                feed_content = generate_site_feed(slug, sites[slug], site_items)
                feed_file = os.path.join(feeds_path, f"site_{slug}.xml")

                with open(feed_file, 'w', encoding='utf-8') as f:
                    f.write(feed_content)
                site_feeds_generated += 1
            except Exception as e:
                print(f"ERROR generating feed for {slug}: {e}")

    # Generate Bundesland feeds
    items_by_bundesland: Dict[str, List[Dict[str, Any]]] = {}
    for item in active_items:
        bl = item.get("bundesland")
        if bl:
            items_by_bundesland.setdefault(bl, []).append(item)

    bl_feeds_generated = 0
    for bundesland, bl_items in items_by_bundesland.items():
        try:
            feed_content = generate_bundesland_feed(bundesland, bl_items, sites)
            feed_file = os.path.join(feeds_path, f"DE-{slugify(bundesland)}.xml")

            with open(feed_file, 'w', encoding='utf-8') as f:
                f.write(feed_content)
            bl_feeds_generated += 1
        except Exception as e:
            print(f"ERROR generating feed for {bundesland}: {e}")

    print(f"Generated {site_feeds_generated} site feeds, {bl_feeds_generated} Bundesland feeds")
    return True


# ======================================================================================================================
# Main Application
# ======================================================================================================================

def load_config(config_path: str = "config.yml") -> Dict[str, Any]:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Could not load config: {e}")
        return {}


async def main():
    """Main application entry point"""
    parser = argparse.ArgumentParser(description="DE-Plan Feed Watcher")
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument("--config", default="config.yml", help="Config file path")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    if not config:
        print("ERROR: Could not load configuration")
        return 1

    storage_path = config.get("storage_path", DEFAULT_STORAGE)
    feeds_path = config.get("feeds_path", DEFAULT_FEEDS)

    # Create directories
    if not ensure_dir(storage_path) or not ensure_dir(feeds_path):
        print("ERROR: Could not create required directories")
        return 1

    # Load state
    state = load_state(storage_path)
    initial_sites = len(state.get("sites", {}))
    initial_items = len(state.get("items", []))
    print(f"Loaded state: {initial_sites} sites, {initial_items} items")

    # Parse site configurations
    site_configs = []
    for site_data in config.get("sites", []):
        if site_data.get("url") and site_data.get("name"):  # Skip empty entries
            site_configs.append(SiteConfig(
                name=site_data["name"],
                bundesland=site_data.get("bundesland", "Unknown"),
                url=site_data["url"],
                selectors=site_data.get("selectors", []),
                mode=site_data.get("mode", "text"),
                update_frequency=site_data.get("update_frequency", "normal"),
                priority=site_data.get("priority", "normal"),
            ))

    if not site_configs:
        print("ERROR: No valid site configurations found")
        return 1

    print(f"Processing {len(site_configs)} sites...")

    # Process all sites
    user_agent = config.get("user_agent", "DE-Plan-Feed-Watcher/2.0")
    timeout = config.get("site_timeout_sec", 30)

    headers = {"User-Agent": user_agent}
    changes_detected = 0

    async with httpx.AsyncClient(headers=headers) as client:
        tasks = [process_site(state, client, site) for site in site_configs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                changes_detected += 1
            elif isinstance(result, Exception):
                print(f"ERROR: {result}")

    print(f"Changes detected: {changes_detected}")

    # Save state
    if not save_state(storage_path, state):
        print("ERROR: Could not save state")
        return 1

    # Generate RSS feeds
    active_slugs = [slugify(site.name) for site in site_configs]
    if not generate_all_feeds(state, feeds_path, active_slugs):
        print("ERROR: Could not generate feeds")
        return 1

    final_sites = len(state.get("sites", {}))
    final_items = len(state.get("items", []))
    print(f"Final state: {final_sites} sites, {final_items} items")
    print("Scraping completed successfully")

    return 0


if __name__ == "__main__":
    import sys

    sys.exit(asyncio.run(main()))
