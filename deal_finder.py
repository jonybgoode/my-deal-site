#!/usr/bin/env python3
"""
deal_finder.py — Minimal viable automated deal-finding pipeline.

Pipeline (one pass):
    1. Ingest deals from the built-in 14-source registry (RSS feeds +
       lightweight HTML/JSON/Playwright scrapes; 15s budget per source,
       failures are non-fatal), falling back to the mock affiliate feed
       when every source comes up dry.
    2. Normalize everything into a standard `Deal` dataclass, then enrich
       prices via enrich_deal(): real Amazon prices with --scrape
       (Playwright, budget-capped by MAX_SCRAPE), mock PA-API lookup
       otherwise.
    3. Store deals + every price check in SQLite.
    4. Score deals:  qualifies if discount >= 20% OR price is the all-time
                     low (local price_history + backfilled Keepa-style
                     external history for new deals).
                     score = discount_percent + (10 if all-time low else 0)
                     only deals with score >= 15 make the page.
    5. Deduplicate across sources (rapidfuzz: title match >= 85 AND price
       within 5%), then render qualifying deals to static HTML pages —
       deals without a promo code -> deals.html, deals WITH one ->
       coupons.html. Both are the mandatory output of every run.
    6. Optional Telegram side-channel: if TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID
       are set, deals with score >= 40 are also pushed to Telegram.

Usage:
    python deal_finder.py --dry-run          # write deals-preview.html, no DB alert-marking
    python deal_finder.py --once             # single pass, then exit (cron mode)
    python deal_finder.py                    # loop forever every INTERVAL_MINUTES
    python deal_finder.py --dry-run --open   # render + open the preview in a browser
    python deal_finder.py --once --scrape    # single pass with REAL Amazon prices
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import math
import os
import random
import re
import sqlite3
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlparse

import feedparser
import requests
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Playwright is optional at runtime (RSS/affiliate-only setups still work).
try:
    from playwright.async_api import async_playwright

    PLAYWRIGHT_AVAILABLE = True
except ImportError:  # pragma: no cover
    PLAYWRIGHT_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging / constants
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
)
log = logging.getLogger("deal_finder")

# Cap per feed so a single noisy source can't flood the DB in the MVP.
MAX_ITEMS_PER_FEED = 25

# User-Agent for RSS/API requests (identify yourself politely).
HTTP_UA = "Mozilla/5.0 (compatible; DealFinderMVP/1.0; +https://example.com/bot)"

# --- Scraping selectors (tune these per target site) -----------------------
PRICE_SELECTORS = [
    '[data-testid="product-price"]',
    '[data-testid="price"]',
    ".price",
    ".product-price",
]
# Playwright text-matches() takes a JS regex; "." instead of "\s" avoids
# Python/selector escaping headaches.
REVEAL_BUTTON_SELECTORS = [
    'button:text-matches("reveal.{0,3}code", "i")',
    'a:text-matches("reveal.{0,3}code", "i")',
    'button:has-text("Reveal Code")',
]
CODE_SELECTORS = (
    '[data-testid="promo-code"], .promo-code, .revealed-code, .coupon-code'
)
# Regex run over XHR/fetch JSON bodies to sniff promo codes from network traffic.
CODE_JSON_RE = re.compile(
    r'"(?:promo_?code|coupon_?code|discount_?code)"\s*:\s*"([A-Z0-9]{3,20})"',
    re.IGNORECASE,
)

# --- RSS text mining --------------------------------------------------------
# Matches "$1,299.99", "£49", "€ 12.50" etc.
CURRENCY_RE = re.compile(r"[$£€]\s?(\d{1,6}(?:,\d{3})*(?:\.\d{1,2})?)")
# Matches "Code: SAVE20", "promo code ABC123", "coupon XYZ9".
PROMO_CODE_RE = re.compile(
    r"\b(?:promo|coupon|code)\s*[:#-]?\s*([A-Z0-9]{4,15})\b", re.IGNORECASE
)
# First <img src="..."> in an RSS summary (image extraction fallback).
IMG_SRC_RE = re.compile(r"<img[^>]+src=[\"']([^\"']+)[\"']", re.IGNORECASE)
# og:image meta tag on a deal's landing page — the image fallback when a
# feed carries no image data at all (e.g. Slickdeals' text-only summaries).
OG_IMAGE_RE = re.compile(
    r"<meta[^>]+(?:property=[\"']og:image[\"'][^>]*content=[\"']([^\"']+)[\"']"
    r"|content=[\"']([^\"']+)[\"'][^>]*property=[\"']og:image[\"'])",
    re.IGNORECASE,
)
# Free-shipping thresholds ("free shipping on $35+", "$25+ orders") are not
# product prices — strip them before mining prices from RSS text, otherwise
# the threshold becomes a fake "original price" (e.g. $2.20 trowel "was $35").
SHIPPING_THRESHOLD_RE = re.compile(
    r"(?:(?:w/|with|over|on(?:\s+orders)?\s+of)\s+)?[$£€]\s?\d{1,6}(?:[.,]\d{1,2})?\s*\+",
    re.IGNORECASE,
)
# Raw Amazon product URL — see AMAZON_ASIN_RE at the source registry below.
# Realistic desktop UA for Amazon scraping — the polite bot UA (HTTP_UA)
# gets an instant captcha wall; this one passes (verified 2026-11).
AMAZON_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)
# Current-price selectors on an Amazon product page, most specific first.
# .a-text-price marks struck-through OLD prices, so it is excluded until
# the explicit list-price selectors below.
AMAZON_PRICE_SELECTORS = [
    "#corePriceDisplay_desktop_feature_div .a-offscreen",
    ".a-price:not(.a-text-price) > .a-offscreen",
    ".a-price .a-offscreen",
]
# List/original ("was $X") price selectors on an Amazon product page.
AMAZON_LIST_PRICE_SELECTORS = [
    ".basisPrice .a-offscreen",
    "#listPrice",
    ".a-price.a-text-price .a-offscreen",
]
# Slickdeals "Get Deal" tracking hop (the deal's outbound link). The
# endpoint 403s plain HTTP clients — it must be followed by a real browser
# (Playwright), which is exactly how a human user reaches the merchant.
SLICKDEALS_CLICK_RE = re.compile(r"https://slickdeals\.net/click\?[^\"'\s<>]+")

# Raw Amazon product URL (handles /dp/ASIN, /gp/product/ASIN, brand-slugged
# /Some-Brand/dp/ASIN and legacy /exec/obidos/ASIN/... links used by some
# deal aggregators). Also matches inside Playwright's wait_for_url.
AMAZON_ASIN_RE = re.compile(
    r"amazon\.com/(?:gp/(?:product|aw)/|exec/obidos/ASIN/"
    r"|(?:[a-z0-9-]+/)?dp/)([A-Z0-9]{10})",
    re.IGNORECASE,
)

# Per-source budget (seconds). One slow/hung site must never stall a run.
SOURCE_TIMEOUT = int(os.getenv("SOURCE_TIMEOUT", "15"))

# Deals from sources that merely aggregate Amazon get deduplicated against
# each other: same product (fuzzy title match) + price within 5%.
try:
    from rapidfuzz import fuzz

    RAPIDFUZZ_AVAILABLE = True
except ImportError:  # pragma: no cover
    RAPIDFUZZ_AVAILABLE = False

# Titles are normalized (lowercase, strip punctuation/whitespace) before
# fuzzy comparison.
DEDUP_NOISE_RE = re.compile(r"[^a-z0-9 ]+")
# 1Sale embeds its deal list as Next.js flight JSON with these escapes.
JSON_ESCAPES = [("\\u0026", "&"), ('\\"', '"')]

# Generic <a href="...">text</a> scraper for plain-HTML sources.
ANCHOR_RE = re.compile(
    r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class SourceSpec:
    """One deal source: an RSS feed or a lightweight scrape."""

    name: str
    kind: str   # "rss" | "html" | "json" | "playwright"
    url: str
    # For kind="html": only anchors whose href matches this regex are kept.
    href_pattern: str = ""


# The 14-source registry (probed & verified 2026-11). Sources that fail or
# return nothing simply log a WARNING and the run continues.
SOURCES: list[SourceSpec] = [
    SourceSpec("slickdeals", "rss",
               "https://slickdeals.net/newsearch.php?mode=frontpage&searcharea=deals&searchin=first&rss=1&q=amazon"),
    SourceSpec("edealinfo", "rss", "https://www.edealinfo.com/rss/latest.xml"),
    SourceSpec("musiciansfriend", "playwright",
               "https://www.musiciansfriend.com/stupid-deal-of-the-day"),
    SourceSpec("bensbargains", "rss", "https://bensbargains.com/rss"),
    SourceSpec("dealnews", "rss", "https://www.dealnews.com/?rss=1&sort=hotness"),
    SourceSpec("dealsofamerica", "rss", "https://www.dealsofamerica.com/rss.xml"),
    SourceSpec("gottadeal", "html", "https://gottadeal.com/", href_pattern=r"/deal/\d"),
    SourceSpec("dealcatcher", "rss", "https://www.dealcatcher.com/rss"),
    SourceSpec("dealsdigger", "html", "https://www.dealsdigger.com/deals",
               href_pattern=r"amazon\.com/exec/obidos/ASIN/"),
    SourceSpec("1sale", "json", "https://1sale.com/"),
    SourceSpec("ihotoffers", "playwright", "https://ihotoffers.com/"),
    SourceSpec("shopsale", "playwright", "https://shopsale.com/"),
    SourceSpec("techbargains", "rss", "https://www.techbargains.com/rss.xml"),
    SourceSpec("myvipon", "playwright", "https://www.myvipon.com/"),
]


# ---------------------------------------------------------------------------
# Standard deal model
# ---------------------------------------------------------------------------
@dataclass
class Deal:
    id: Optional[int]                     # DB row id; None until stored
    title: str
    url: str
    merchant: str
    price: Optional[float]
    original_price: Optional[float]
    discount_percent: float
    promo_code: Optional[str]
    image_url: Optional[str] = None       # product photo for the HTML page
    source: str = "unknown"               # source name ("slickdeals", "1sale", ...)
    found_at: str = ""                    # ISO-8601 UTC
    score: float = 0.0                    # pipeline score (set before dedup)
    category: str = "Other"               # broad bucket from categorize_deal()


def now_iso() -> str:
    """Current UTC time as an ISO-8601 string (sortable, SQLite-friendly)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def to_float(value) -> Optional[float]:
    """Best-effort float conversion; returns None instead of raising."""
    if value is None or value == "":
        return None
    try:
        return round(float(str(value).replace(",", "").replace("$", "")), 2)
    except (TypeError, ValueError):
        return None


def merchant_from_url(url: str) -> str:
    """Derive a merchant name from a URL's hostname (www stripped)."""
    try:
        host = urlparse(url).netloc or ""
        return host.removeprefix("www.").split(":")[0] or "unknown"
    except Exception:
        return "unknown"


def extract_prices(text: str) -> tuple[Optional[float], Optional[float]]:
    """
    Pull (current_price, original_price) out of free text like:
        "Sony WH-1000XM5 $248 + free shipping (reg. $399.99)"
    Heuristic: first price = current price, next *larger* value = original.
    """
    matches = CURRENCY_RE.findall(text or "")
    values = [float(m.replace(",", "")) for m in matches]
    if not values:
        return None, None
    price = values[0]
    original = next((v for v in values[1:] if v > price), None)
    return price, original


def extract_promo_code(text: str) -> Optional[str]:
    """Sniff a promo code out of free text; requires at least one digit to
    reduce false positives on words like 'code'."""
    m = PROMO_CODE_RE.search(text or "")
    if m and any(c.isdigit() for c in m.group(1)):
        return m.group(1).upper()
    return None


def extract_image(entry) -> Optional[str]:
    """
    Best-effort product image extraction. Accepts either:
      - a raw dict from the affiliate JSON feed (checks entry["image_url"]), or
      - a feedparser entry (checks media_content, media_thumbnail and
        enclosures for an item with a URL and a type starting with "image").
    Fallback: the first <img src="..."> inside entry.summary.
    Returns None when nothing is found. Never raises.

    (feedparser entries are dict-like, so .get() works uniformly here.)
    """
    try:
        if not isinstance(entry, dict):
            return None

        # 1) Affiliate JSON feed dict.
        url = entry.get("image_url")
        if url and str(url).startswith("http"):
            return url

        # 2) feedparser media namespaces + enclosures (type must be image/*).
        for key in ("media_content", "media_thumbnail"):
            for media in entry.get(key) or []:
                media_url = media.get("url")
                media_type = str(media.get("type", "") or "")
                if media_url and media_type.startswith("image"):
                    return media_url
        for enclosure in entry.get("enclosures") or []:
            if (
                enclosure.get("href")
                and str(enclosure.get("type", "") or "").startswith("image")
            ):
                return enclosure["href"]

        # 3) First inline <img> in the summary/description.
        match = IMG_SRC_RE.search(entry.get("summary") or "")
        if match:
            return match.group(1)
    except Exception:
        pass  # image extraction must never break normalization
    return None


# ---------------------------------------------------------------------------
# Storage (SQLite)
# ---------------------------------------------------------------------------
def get_db() -> sqlite3.Connection:
    """Open (and initialize) the SQLite database."""
    db_path = os.getenv("DATABASE_PATH", "deals.db")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")  # tolerates overlapping cron runs
    conn.row_factory = sqlite3.Row
    init_db(conn)
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables if they don't exist. Trivial to migrate later."""
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS deals (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            url              TEXT UNIQUE NOT NULL,
            title            TEXT NOT NULL,
            merchant         TEXT,
            price            REAL,            -- latest observed price
            original_price   REAL,
            discount_percent REAL DEFAULT 0,
            promo_code       TEXT,
            source           TEXT,
            found_at         TEXT,            -- first time we saw this deal
            last_alert_price REAL             -- price at last publish; gates the
                                               -- Telegram side-channel + future
                                               -- "NEW since last run" logic
        );

        CREATE TABLE IF NOT EXISTS price_history (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id    INTEGER NOT NULL REFERENCES deals(id),
            price      REAL NOT NULL,
            checked_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_price_history_deal_time
            ON price_history(deal_id, checked_at);
        """
    )
    conn.commit()


def store_deal(conn: sqlite3.Connection, deal: Deal) -> None:
    """
    Insert the deal if it's new (dedup on URL), otherwise refresh its latest
    fields. Always append the price check to price_history. Sets deal.id.
    """
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO deals
            (url, title, merchant, price, original_price, discount_percent,
             promo_code, source, found_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            deal.url, deal.title, deal.merchant, deal.price,
            deal.original_price, deal.discount_percent, deal.promo_code,
            deal.source, deal.found_at,
        ),
    )
    if cur.rowcount == 1:  # brand-new deal
        deal.id = cur.lastrowid
    else:  # seen before: keep the row, refresh the latest snapshot fields
        row = conn.execute(
            "SELECT id, found_at FROM deals WHERE url = ?", (deal.url,)
        ).fetchone()
        deal.id = row[0]
        if row[1]:
            # Keep the FIRST-seen timestamp on the in-memory deal so the
            # NEW badge reflects true age, not this run's normalize time.
            deal.found_at = row[1]
        conn.execute(
            """
            UPDATE deals SET title = ?, merchant = ?, price = ?,
                             original_price = ?, discount_percent = ?,
                             promo_code = ?
            WHERE id = ?
            """,
            (
                deal.title, deal.merchant, deal.price, deal.original_price,
                deal.discount_percent, deal.promo_code, deal.id,
            ),
        )

    # Every observation lands in price_history — the backbone of scoring.
    if deal.price is not None:
        conn.execute(
            "INSERT INTO price_history (deal_id, price, checked_at) VALUES (?, ?, ?)",
            (deal.id, deal.price, now_iso()),
        )
    conn.commit()


def get_price_history(
    conn: sqlite3.Connection, deal_id: int
) -> list[tuple[str, float]]:
    """
    ALL price observations for a deal, oldest first — no time-window cap,
    so 'all-time low' stats and the sparkline cover the deal's full life
    (local observations + backfilled external history alike).
    """
    rows = conn.execute(
        "SELECT checked_at, price FROM price_history "
        "WHERE deal_id = ? ORDER BY checked_at ASC",
        (deal_id,),
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def generate_sparkline(
    points: list[tuple[str, float]], width: int = 100, height: int = 28
) -> str:
    """
    Tiny SVG line chart of a deal's price history (inline-safe markup).

    Downsampling: more than 60 points -> keep every Nth so the path stays
    light (365 backfilled days collapse to ~60 points). Fewer than 2
    points -> empty string (nothing to draw yet).
    """
    if len(points) < 2:
        return ""
    if len(points) > 60:
        step = math.ceil(len(points) / 60)
        points = points[::step]

    prices = [p for _, p in points]
    lo, hi = min(prices), max(prices)
    if hi - lo < 1e-9:  # flat series — give it a hair of range to draw
        hi = lo + 1.0

    n = len(points)
    coords = []
    for i, (_, price) in enumerate(points):
        x = (i / (n - 1)) * width
        y = height - ((price - lo) / (hi - lo)) * (height - 2) - 1
        coords.append(f"{x:.1f},{y:.1f}")
    path_d = "M" + " L".join(coords)

    return (
        f'<svg viewBox="0 0 {width} {height}" preserveAspectRatio="none" '
        f'role="img" aria-label="price history">'
        f'<path d="{path_d}" fill="none" stroke="#22c55e" stroke-width="1.5" '
        f'vector-effect="non-scaling-stroke"/></svg>'
    )


def fetch_external_history(
    asin: str, base_price: Optional[float] = None
) -> list[dict]:
    """
    Fetch a product's historical prices from an external price tracker.

    PRODUCTION (Keepa — camelcamelcamel has no public API):
        GET https://api.keepa.com/product
            ?key=YOUR_KEEPA_KEY&domain=1&asin={asin}&history=1
        Keepa returns 1-hour-resolution arrays (csv[0] = new, csv[1] =
        used); downsample to daily MINs and map to [{"date", "price"}].
        Requires a paid key — https://keepa.com/#!api.

    MOCK (no paid key — deterministic per ASIN so every run regenerates
    the identical series): 365 daily points ending yesterday. Base price
    $299 (or the deal's current price when known) with random ±15%
    fluctuations plus 2-4 multi-day "deep dip" sales at 55-75% of base.
    """
    rng = random.Random(f"keepa-mock:{asin}")
    base = float(base_price) if base_price else 299.0
    end = datetime.now(timezone.utc) - timedelta(days=1)

    out: list[dict] = []
    for i in range(365, 0, -1):
        day = end - timedelta(days=i - 1)
        price = base * (1 + rng.uniform(-0.15, 0.15))
        out.append({"date": day.isoformat(timespec="seconds"),
                    "price": round(price, 2)})

    # A few deep dips (lightning-deal style) scattered mid-year.
    for _ in range(rng.randint(2, 4)):
        start = rng.randrange(60, len(out) - 6)
        dip = rng.uniform(0.55, 0.75)
        for j in range(start, start + rng.randint(2, 5)):
            out[j]["price"] = round(base * dip, 2)
    return out


def backfill_external_history(conn: sqlite3.Connection, deal: Deal) -> int:
    """
    Give brand-new deals (or any deal with < 5 local data points) a year
    of external history in one bulk insert, so 'all-time low' badges and
    sparklines work from the very first render. Self-gating: once a deal
    has >= 5 points it is never backfilled again.

    External key: the ASIN for Amazon deals; otherwise a stable hash of
    the deal URL (keeps non-Amazon deals testable with the mock source).
    """
    if deal.id is None:
        return 0
    n_local = conn.execute(
        "SELECT COUNT(*) FROM price_history WHERE deal_id = ?", (deal.id,)
    ).fetchone()[0]
    if n_local >= 5:
        return 0

    match = AMAZON_ASIN_RE.search(unquote(deal.url))
    key = match.group(1) if match else hashlib.sha1(
        deal.url.encode("utf-8")
    ).hexdigest()[:10]

    rows = fetch_external_history(key, base_price=deal.price)
    if not rows:
        return 0
    conn.executemany(
        "INSERT INTO price_history (deal_id, price, checked_at) VALUES (?, ?, ?)",
        [(deal.id, r["price"], r["date"]) for r in rows],
    )
    conn.commit()
    log.info(
        "Backfilled %d days of external history for %s", len(rows), deal.title[:60]
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Sources: RSS feeds + lightweight scrapes (14-source registry)
# ---------------------------------------------------------------------------
def _fetch_url(url: str, timeout: Optional[int] = None) -> requests.Response:
    """
    GET a URL with SOURCE_TIMEOUT budget and a UA fallback: the polite bot
    UA first; sites that 403 it (DealNews) get the browser UA.
    """
    last_exc: Optional[Exception] = None
    for ua in (HTTP_UA, AMAZON_UA):
        try:
            resp = requests.get(url, timeout=timeout or SOURCE_TIMEOUT,
                                headers={"User-Agent": ua})
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
    raise last_exc  # type: ignore[misc]


def fetch_rss_source(name: str, url: str) -> list[dict]:
    """
    Fetch one RSS/Atom feed and return semi-normalized raw deals.
    Uses requests (for timeouts) and feeds the body to feedparser.
    """
    log.info("Fetching RSS feed [%s]: %s", name, url)
    resp = _fetch_url(url)
    parsed = feedparser.parse(resp.content)

    if not parsed.entries:
        # 0 entries usually means a dead URL, a redirect to HTML, or
        # malformed XML — surface it loudly so it can be diagnosed.
        log.warning(
            "RSS feed %s returned 0 entries (HTTP %s, feedparser bozo=%s) "
            "— check the URL / feed format",
            url, resp.status_code, getattr(parsed, "bozo", None),
        )
        return []

    items: list[dict] = []
    for entry in parsed.entries[:MAX_ITEMS_PER_FEED]:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link:
            continue
        # Mine the title + description for prices and promo codes.
        # Shipping thresholds ("on $35+") are stripped first so
        # they don't pollute the price/original-price heuristics.
        blob = f"{title} {entry.get('summary', '')}"
        blob = SHIPPING_THRESHOLD_RE.sub(" ", blob)
        price, original = extract_prices(blob)
        items.append(
            {
                "title": title,
                "url": link,
                # Deal titles usually name the store; fall back to
                # the link's domain, then the feed's own name.
                "merchant": (
                    merchant_from_url(link)
                    or parsed.feed.get("title", "unknown")
                ),
                "price": price,
                "original_price": original,
                "promo_code": extract_promo_code(blob),
                # Keep the original feedparser entry around so
                # normalize_deal() can pull a product image via
                # extract_image() (media_content/enclosures/img).
                "entry": entry,
            }
        )
    return items


def scrape_html_source(spec: SourceSpec) -> list[dict]:
    """
    Plain-HTML source (requests + regex, no JS): collect anchors whose href
    matches the source's pattern; the anchor text is the deal title, mined
    for prices like any RSS blob.
    """
    from urllib.parse import urljoin

    log.info("Scraping HTML source [%s]: %s", spec.name, spec.url)
    resp = _fetch_url(spec.url)
    base = str(resp.url)

    pattern = re.compile(spec.href_pattern, re.IGNORECASE) if spec.href_pattern else None
    seen: set[str] = set()
    items: list[dict] = []
    for href, inner in ANCHOR_RE.findall(resp.text):
        url = urljoin(base, href.strip())
        title = TAG_RE.sub(" ", inner)
        title = re.sub(r"\s+", " ", title).strip()
        if not title or len(title) < 8 or url in seen:
            continue
        if pattern and not pattern.search(url):
            continue
        seen.add(url)
        price, original = extract_prices(
            SHIPPING_THRESHOLD_RE.sub(" ", title)
        )
        items.append(
            {
                "title": title,
                "url": url,
                "merchant": merchant_from_url(url),
                "price": price,
                "original_price": original,
                "promo_code": extract_promo_code(title),
            }
        )
        if len(items) >= MAX_ITEMS_PER_FEED:
            break
    return items


# 1Sale embeds deals as Next.js flight JSON. After unescaping, each deal
# object has these fields in a stable order (no nested {} between them).
ONESALE_DEAL_RE = re.compile(
    r'"title":"(.*?)"[^{}]*?"image":"([^"]*)"[^{}]*?'
    r'"soldOut":(true|false)[^{}]*?'
    r'"price":([\d.]+),"originalPrice":([\d.]+|null)[^{}]*?'
    r'"dealUrl":"(.*?)"'
)


def scrape_json_source(spec: SourceSpec) -> list[dict]:
    """JSON-in-HTML source (1Sale): mine the Next.js flight-data payload."""
    log.info("Scraping JSON source [%s]: %s", spec.name, spec.url)
    resp = _fetch_url(spec.url)
    text = resp.text
    for old, new in JSON_ESCAPES:
        text = text.replace(old, new)

    items: list[dict] = []
    for m in ONESALE_DEAL_RE.finditer(text):
        (title, image, sold_out, price_s, original_s, deal_url) = m.groups()
        if sold_out == "true":
            continue
        title = title.strip()
        # Titles cut short by an escaped quote truncate to junk
        # ("15.6\\", "Warming Mat for Food (24\\") — require three real
        # words and no unresolved escape characters.
        if (
            not title
            or "\\" in title
            or len(title.split()) < 3
            or not deal_url.startswith("http")
        ):
            continue
        # promoCode sits right after dealUrl inside the same object.
        tail = text[m.end():m.end() + 80]
        pm = re.match(r'[^{}]*?"promoCode":"([^"]+)"', tail)
        promo = pm.group(1).strip() if pm else None
        items.append(
            {
                "title": title,
                "url": deal_url,
                "merchant": merchant_from_url(deal_url),
                "price": to_float(price_s),
                "original_price": to_float(original_s),
                "promo_code": promo or extract_promo_code(title),
                "image_url": image if image.startswith("http") else None,
            }
        )
        if len(items) >= MAX_ITEMS_PER_FEED:
            break
    return items


async def _scrape_playwright_sources(
    specs: list[SourceSpec],
) -> dict[str, list[dict]]:
    """
    JS-heavy sources: one shared headless Chromium, one page per source.
    Returns {source_name: raw deal dicts}. Per-site extraction follows what
    each SPA actually renders (probed 2026-11).
    """
    results: dict[str, list[dict]] = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for spec in specs:
                page = await browser.new_page(user_agent=AMAZON_UA)
                try:
                    log.info("Scraping page [%s]: %s", spec.name, spec.url)
                    await page.goto(
                        spec.url, wait_until="domcontentloaded",
                        timeout=SOURCE_TIMEOUT * 1000,
                    )
                    await page.wait_for_timeout(3000)  # let the SPA hydrate

                    if spec.name == "shopsale":
                        # Anchor text = "$34.99<title>" (price prefixed).
                        raw = await page.evaluate(
                            """() => Array.from(
                                 document.querySelectorAll('a[href*="/products/"]')
                               ).map(a => ({url: a.href,
                                           text: (a.textContent || '').trim()}))"""
                        )
                        items = []
                        for r in raw:
                            text = re.sub(r"\s+", " ", r["text"]).strip()
                            price, original = extract_prices(text)
                            title = CURRENCY_RE.sub(" ", text)
                            title = re.sub(r"\s{2,}", " ", title).strip()
                            if title and len(title) > 8:
                                items.append({
                                    "title": title, "url": r["url"],
                                    "merchant": "shopsale.com",
                                    "price": price,
                                    "original_price": original,
                                    "promo_code": None,
                                })
                        results[spec.name] = items[:MAX_ITEMS_PER_FEED]

                    elif spec.name == "ihotoffers":
                        # /offer/<slug> anchors say "View Details"; the
                        # title lives in the surrounding card container.
                        raw = await page.evaluate(
                            """() => Array.from(
                                 document.querySelectorAll('a[href*="/offer/"]')
                               ).map(a => {
                                 let el = a;
                                 for (let i = 0; i < 4 && el.parentElement; i++) {
                                   el = el.parentElement;
                                   const t = (el.textContent || '').trim();
                                   if (t.length > 40) break;
                                 }
                                 return {url: a.href,
                                         text: (el.textContent || '').trim()};
                               })"""
                        )
                        items = []
                        for r in raw:
                            text = r["text"].replace("View Details", " ")
                            text = re.sub(r"\s+", " ", text).strip()
                            price, original = extract_prices(text)
                            title = CURRENCY_RE.sub(" ", text)
                            title = re.sub(r"\s{2,}", " ", title).strip()
                            if title and len(title) > 8:
                                items.append({
                                    "title": title, "url": r["url"],
                                    "merchant": "ihotoffers.com",
                                    "price": price,
                                    "original_price": original,
                                    "promo_code": extract_promo_code(text),
                                })
                        results[spec.name] = items[:MAX_ITEMS_PER_FEED]

                    else:
                        # Generic fallback (musiciansfriend / myvipon):
                        # only anchors that point at deal-like URLs
                        # (/product/, /deal/, /offer/) — a bare text-length
                        # filter would pull nav & footer junk, and on this
                        # platform /promotion/ links are category hubs
                        # ("See More Trending Deals"), not deals.
                        raw = await page.evaluate(
                            """() => Array.from(document.querySelectorAll(
                                 "a[href*='/product/'], a[href*='/deal/'], "
                                 + "a[href*='/offer/']"))
                               .map(a => ({url: a.href,
                                           text: (a.textContent || '')
                                                  .replace(/\\s+/g, ' ').trim()}))
                               .filter(r => r.text.length > 25
                                            && r.text.length < 200)"""
                        )
                        results[spec.name] = [
                            {
                                "title": r["text"], "url": r["url"],
                                "merchant": merchant_from_url(r["url"]),
                                "price": None, "original_price": None,
                                "promo_code": None,
                            }
                            for r in raw[:MAX_ITEMS_PER_FEED]
                        ]
                except Exception as exc:
                    log.warning("Playwright source %s failed: %s", spec.name, exc)
                    results[spec.name] = []
                finally:
                    await page.close()
        finally:
            await browser.close()
    return results


def fetch_all_sources() -> list[tuple[dict, str]]:
    """
    Fan out to the full 14-source registry (+ any extra RSS_FEEDS URLs not
    already covered). Each source is isolated: one dead site costs a
    WARNING, never the run. Returns (raw_dict, source_name) pairs.
    """
    # Playwright sources share one browser session.
    pw_specs = [s for s in SOURCES if s.kind == "playwright"]
    pw_raw: dict[str, list[dict]] = {}
    if pw_specs:
        if PLAYWRIGHT_AVAILABLE:
            try:
                pw_raw = asyncio.run(_scrape_playwright_sources(pw_specs))
            except Exception as exc:
                log.warning("Playwright source batch failed: %s", exc)
        else:
            log.warning(
                "%d Playwright sources skipped (playwright not installed)",
                len(pw_specs),
            )

    raw_items: list[tuple[dict, str]] = []
    ok_sources = 0
    for spec in SOURCES:
        items: list[dict] = []
        try:
            if spec.kind == "rss":
                items = fetch_rss_source(spec.name, spec.url)
            elif spec.kind == "html":
                items = scrape_html_source(spec)
            elif spec.kind == "json":
                items = scrape_json_source(spec)
            elif spec.kind == "playwright":
                items = pw_raw.get(spec.name, [])
        except Exception as exc:
            log.warning(
                "Source %s (%s) failed: %s — continuing", spec.name,
                spec.kind, exc,
            )
        if items:
            ok_sources += 1
        else:
            log.warning("Source %s returned 0 items", spec.name)
        log.info("Source %s: %d items", spec.name, len(items))
        raw_items.extend((item, spec.name) for item in items)

    # Extra feeds from RSS_FEEDS that the registry doesn't already cover.
    registry_urls = {s.url for s in SOURCES}
    extras = [
        u.strip() for u in os.getenv("RSS_FEEDS", "").split(",") if u.strip()
    ]
    extra_count = 0
    for url in extras:
        if url in registry_urls:
            continue
        try:
            items = fetch_rss_source("rss-extra", url)
            raw_items.extend((item, "rss-extra") for item in items)
            extra_count += 1
        except Exception as exc:
            log.warning("Extra feed %s failed: %s", url, exc)

    log.info(
        "Fetched %d raw items from %d/%d sources (+%d extra feeds)",
        len(raw_items), ok_sources, len(SOURCES), extra_count,
    )
    return raw_items


# ---------------------------------------------------------------------------
# Source 2: affiliate network feed (placeholder / mock)
# ---------------------------------------------------------------------------
def fetch_affiliate(feed_path: str) -> list[dict]:
    """
    Placeholder for a real affiliate-network product feed (Awin, CJ,
    Impact, Rakuten...). Real implementations would call e.g.:

        - Awin:    GET https://productdata.awin.com/datafeed/download/...
                   (api key + feed IDs as query params, CSV/TSV/XML)
        - CJ:      GET https:// advertiser-lookup-api.cj.com/v2/... with
                   your API key header
                   (details: https://developers.cj.com/)
        - Impact:  GET https://api.impact.com/... with Basic auth

    For the MVP we read a local sample JSON file with the same shape a real
    fetcher would normalize to.
    """
    if not os.path.exists(feed_path):
        log.warning("Affiliate feed file not found (%s); skipping", feed_path)
        return []
    try:
        with open(feed_path, encoding="utf-8") as f:
            data = json.load(f)
        products = data.get("products", [])
        log.info("Affiliate feed: %d products", len(products))
        return [
            {
                "title": p.get("product_name") or p.get("name", ""),
                "url": p.get("url", ""),
                "merchant": p.get("merchant", "unknown"),
                "price": to_float(p.get("search_price")),
                "original_price": to_float(p.get("retail_price")),
                "promo_code": p.get("promo_code"),
                "image_url": p.get("image_url"),
            }
            for p in products
        ]
    except Exception as exc:
        log.error("Affiliate feed failed for %s: %s", feed_path, exc)
        return []


# ---------------------------------------------------------------------------
# Source 3: Playwright scraper (JS-heavy retailer page)
# ---------------------------------------------------------------------------
async def _scrape_sample_async(url: str) -> Optional[dict]:
    """
    Scrape one product page with Playwright (async API):
      1. Read the product price from the DOM (selector list + regex fallback).
      2. Click the "Reveal Code" button and capture the promo code from the
         DOM, or sniff it from JSON network responses.

    NOTE on anti-bot: many retailers run detection (Cloudflare, PerimeterX,
    Akamai). Production-grade scraping usually needs stealth plugins
    (e.g. playwright-stealth), residential proxies, realistic fingerprints,
    human-like delays — and you must respect the site's ToS and robots.txt.
    """
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()

            # Sniff promo codes out of XHR/fetch JSON on the wire.
            net_codes: list[str] = []

            async def _capture_code(response):
                try:
                    if "json" in response.headers.get("content-type", ""):
                        body = await response.text()
                        match = CODE_JSON_RE.search(body)
                        if match:
                            net_codes.append(match.group(1))
                except Exception:
                    pass  # response bodies can vanish; ignore

            page.on("response", _capture_code)

            log.info("Scraping %s", url)
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)

            # --- 1) Price: try known selectors, then regex the whole page.
            price: Optional[float] = None
            for selector in PRICE_SELECTORS:
                locator = page.locator(selector).first
                if await locator.count():
                    try:
                        text = await locator.inner_text(timeout=3000)
                        price = extract_prices(text)[0]
                        if price is not None:
                            break
                    except Exception:
                        continue
            if price is None:
                body_text = await page.inner_text("body")
                price = extract_prices(body_text)[0]

            # --- 2) Click "Reveal Code" like a real user, read the result.
            code: Optional[str] = None
            for selector in REVEAL_BUTTON_SELECTORS:
                button = page.locator(selector).first
                if await button.count():
                    try:
                        await button.scroll_into_view_if_needed(timeout=3000)
                        await button.click(timeout=5000)  # real user click
                        # Wait for a code element to render after the click.
                        await page.wait_for_selector(
                            CODE_SELECTORS, state="visible", timeout=8000
                        )
                        code = (
                            await page.locator(CODE_SELECTORS).first.inner_text()
                        ).strip()
                        code = code or None
                    except Exception as exc:
                        log.warning("Reveal-code click failed (%s): %s", selector, exc)
                    break

            # Fall back to a code sniffed from network traffic.
            if not code and net_codes:
                code = net_codes[0]

            page_title = await page.title()
            return {
                "title": page_title or url,
                "url": url,
                "merchant": merchant_from_url(url),
                "price": price,
                "original_price": None,
                "promo_code": code,
            }
        finally:
            await browser.close()


def scrape_sample(url: str) -> Optional[dict]:
    """Sync wrapper so the pipeline can call the async scraper easily."""
    return asyncio.run(_scrape_sample_async(url))


# ---------------------------------------------------------------------------
# Amazon price scraping (real, Playwright)
# ---------------------------------------------------------------------------
class AmazonBlockedError(RuntimeError):
    """Amazon served a bot wall (captcha / robot check) or a dead page."""


async def _scrape_amazon_async(
    url: Optional[str] = None, click_hop: Optional[str] = None
) -> dict:
    """
    Scrape the live price off one Amazon product page with Playwright.

    Give EITHER:
      - url:       a direct amazon.com product URL, or
      - click_hop: a deal-site tracking link (e.g. slickdeals.net/click?...)
                   that a real browser follows until it lands on the Amazon
                   product page (the resolved URL is returned).

    Anti-bot basics: realistic desktop UA + viewport + locale, and a random
    2-5 s pause before navigating so back-to-back scrapes don't look
    robotic. Amazon can still serve a captcha wall — that raises
    AmazonBlockedError and the caller falls back to mock prices.

    Returns {"price": float, "original_price": float | None,
             "title": str | None, "resolved_url": str | None}.
    """
    if not url and not click_hop:
        raise ValueError("scrape_amazon_price needs a url or a click_hop")
    start_url = click_hop or url
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            context = await browser.new_context(
                user_agent=AMAZON_UA,
                viewport={"width": 1366, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()

            await asyncio.sleep(random.uniform(2, 5))  # human-like pause

            log.info("Amazon scrape: loading %s", start_url)
            response = await page.goto(
                start_url, wait_until="domcontentloaded", timeout=45000
            )

            resolved_url: Optional[str] = None
            if click_hop:
                # Wait for the hop's JS/redirect chain to land on Amazon.
                try:
                    await page.wait_for_url(AMAZON_ASIN_RE, timeout=25000)
                except Exception:
                    pass  # fall through and inspect wherever we are now
                landed = page.url
                match = AMAZON_ASIN_RE.search(unquote(landed))
                if not match:
                    raise AmazonBlockedError(
                        f"Click hop did not reach an Amazon product page "
                        f"(landed on: {landed[:90]})"
                    )
                resolved_url = f"https://www.amazon.com/dp/{match.group(1)}"
                log.info("Amazon scrape: click hop resolved -> %s", resolved_url)

            if response is not None and response.status in (403, 404, 503):
                raise AmazonBlockedError(f"HTTP {response.status} from Amazon")

            content = await page.content()
            page_title = (await page.title()) or ""
            lowered = (content + page_title).lower()
            if "captcha" in lowered or "robot check" in lowered:
                raise AmazonBlockedError("Captcha detected")
            if "page not found" in page_title.lower():
                raise AmazonBlockedError("Product page not found (dead ASIN?)")

            async def _first_price(selectors: list[str]) -> Optional[float]:
                for selector in selectors:
                    locator = page.locator(selector).first
                    if await locator.count():
                        try:
                            text = await locator.inner_text(timeout=3000)
                            price = extract_prices(text)[0]
                            if price is not None:
                                return price
                        except Exception:
                            continue
                return None

            price = await _first_price(AMAZON_PRICE_SELECTORS)
            if price is None:
                raise AmazonBlockedError(
                    "No price found on the page (layout change or soft block)"
                )
            original_price = await _first_price(AMAZON_LIST_PRICE_SELECTORS)

            title: Optional[str] = None
            title_loc = page.locator("#productTitle")
            if await title_loc.count():
                try:
                    title = (await title_loc.first.inner_text(timeout=3000)).strip()
                except Exception:
                    title = None

            return {
                "price": price,
                "original_price": original_price,
                "title": title or None,
                "resolved_url": resolved_url,
            }
        finally:
            await browser.close()


def scrape_amazon_price(
    url: Optional[str] = None, click_hop: Optional[str] = None
) -> dict:
    """Sync wrapper so the (sync) pipeline can call the async Amazon scraper."""
    return asyncio.run(_scrape_amazon_async(url=url, click_hop=click_hop))


# ---------------------------------------------------------------------------
# Normalization & scoring
# ---------------------------------------------------------------------------
# Broad content buckets used by the on-page category filter. First category
# with a word-boundary keyword match wins (order matters: e.g. "gaming
# laptop" lands in Electronics because Electronics is checked first).
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Electronics": [
        "tv", "television", "headphones", "earbuds", "earphones", "airpods",
        "speaker", "soundbar", "subwoofer", "ssd", "hard drive", "hdd", "nas",
        "laptop", "notebook", "macbook", "monitor", "display", "projector",
        "camera", "camcorder", "drone", "gopro", "tablet", "ipad", "iphone",
        "smartphone", "phone", "pixel", "galaxy", "charger", "power bank",
        "usb", "usb-c", "hdmi", "cable", "router", "wifi", "mesh network",
        "bluetooth", "keyboard", "mouse", "webcam", "gpu", "rtx", "gtx",
        "radeon", "cpu", "processor", "ryzen", "intel core", "ram", "memory",
        "motherboard", "smartwatch", "smart watch", "fitness tracker",
        "fire tv", "roku", "streaming stick", "echo", "alexa", "kindle",
        "e-reader", "printer", "scanner", "battery", "powerline", "adapter",
        "hub", "dock", "sd card", "microsd", "flash drive", "av receiver",
        "soundbar", "turntable", "receiver", "amp",
    ],
    "Gaming": [
        "ps5", "ps4", "playstation", "xbox", "nintendo", "switch", "wii",
        "gamecube", "steam deck", "steam", "gaming", "game", "games",
        "lego", "duplo", "nerf", "board game", "card game", "puzzle",
        "console", "controller", "gamepad", "joystick", "vr", "meta quest",
        "oculus", "roblox", "minecraft", "fortnite", "playstation portal",
    ],
    "Home & Kitchen": [
        "vacuum", "roomba", "shark vacuum", "cookware", "pan", "skillet",
        "pot", "air fryer", "fryer", "instant pot", "pressure cooker",
        "coffee", "espresso", "keurig", "coffee maker", "moka", "blender",
        "mixer", "food processor", "airfryer", "knife", "knives", "cutlery",
        "plates", "dishes", "mugs", "soap", "detergent", "towels", "sheets",
        "bedding", "bed", "pillow", "mattress", "chair", "couch", "sofa",
        "recliner", "table", "desk", "lamp", "candle", "storage",
        "organizer", "cleaning", "laundry", "kitchen", "grill", "smoker",
        "air purifier", "humidifier", "dehumidifier", "space heater",
        "thermostat", "fan", "garden", "planter", "tissues", "toilet paper",
        "paper towels", "sponges", "trash can", "water bottle", "thermos",
    ],
    "Fashion": [
        "shoes", "sneakers", "trainers", "boots", "sandals", "heels",
        "loafers", "shirt", "t-shirt", "tee", "polo", "jacket", "coat",
        "parka", "hoodie", "sweatshirt", "sweater", "dress", "jeans",
        "pants", "trousers", "shorts", "socks", "underwear", "bra",
        "swimsuit", "bikini", "watch", "watches", "sunglasses", "handbag",
        "purse", "wallet", "backpack", "jewelry", "necklace", "earrings",
        "bracelet", "ring", "diamond", "perfume", "cologne", "fragrance",
        "makeup", "lipstick", "cosmetics", "skincare", "serum", "lotion",
        "apparel", "clothing", "sandals",
    ],
}


def categorize_deal(title: str) -> str:
    """
    Assign a broad category to a deal from its title via case-insensitive
    keyword matching (whole-word regex, first matching category wins).
    Anything unrecognized falls back to "Other".
    """
    text = (title or "").lower()
    if text:
        for category, keywords in CATEGORY_KEYWORDS.items():
            for keyword in keywords:
                if re.search(rf"\b{re.escape(keyword)}\b", text):
                    return category
    return "Other"


def normalize_deal(raw: dict, source: str) -> Optional[Deal]:
    """
    Convert any source's raw dict into the standard Deal shape.
    Raw dict contract: title, url, merchant, price, original_price,
    promo_code, image_url (all keys optional except title/url).
    RSS raw dicts may also carry the original feedparser entry under
    "entry" for image extraction.
    """
    title = (raw.get("title") or "").strip()
    url = (raw.get("url") or "").strip()
    if not title or not url:
        return None

    price = to_float(raw.get("price"))
    original = to_float(raw.get("original_price"))
    original = original if (original and price and original > price) else None

    discount = 0.0
    if price is not None and original:
        discount = round((1 - price / original) * 100, 1)

    # Product image: affiliate dicts carry image_url directly; RSS raw dicts
    # carry the original feedparser entry under "entry" for extract_image().
    image_url = extract_image(raw.get("entry") or raw)

    return Deal(
        id=None,
        title=title,
        url=url,
        merchant=(raw.get("merchant") or merchant_from_url(url) or "unknown"),
        price=price,
        original_price=original,
        discount_percent=discount,
        promo_code=raw.get("promo_code") or None,
        image_url=image_url,
        source=source,
        found_at=now_iso(),
        category=categorize_deal(title),
    )


def fetch_landing_page(url: str) -> Optional[str]:
    """
    GET a deal landing page's HTML — one shared fetch feeding both the
    og:image lookup and the cheap raw-href Amazon URL resolution.
    Best-effort: any failure returns None (bot UA, 10 s timeout).
    """
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": HTTP_UA})
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        log.debug("landing page fetch failed for %s: %s", url, exc)
        return None


def extract_og_image(html_text: str) -> Optional[str]:
    """og:image URL out of raw HTML, or None."""
    match = OG_IMAGE_RE.search(html_text)
    if match:
        image = (match.group(1) or match.group(2) or "").strip()
        if image.startswith("http"):
            return image
    return None


def is_direct_amazon(url: str) -> bool:
    """True when the URL already points at an Amazon product page."""
    return bool(AMAZON_ASIN_RE.search(unquote(url)))


def extract_amazon_target(html_text: str) -> Optional[str]:
    """
    Canonical amazon.com/dp/<ASIN> URL from a raw Amazon product href
    embedded in a deal landing page (some deals link straight out).
    Returns None when the page carries no raw Amazon link.
    """
    match = AMAZON_ASIN_RE.search(html_text)
    if match:
        return f"https://www.amazon.com/dp/{match.group(1)}"
    return None


def extract_click_hop(html_text: str) -> Optional[str]:
    """
    First slickdeals.net/click?... tracking link on a landing page
    (HTML-unescaped, ready for a browser to follow).
    """
    match = SLICKDEALS_CLICK_RE.search(html_text)
    if match:
        return html.unescape(match.group(0))
    return None


# Per-run budget for real Amazon scrapes: --scrape hits at most
# MAX_SCRAPE (env, default 10) Amazon pages per pipeline pass so the run
# stays fast and the IP stays off Amazon's block lists.
_SCRAPE_STATE = {"used": 0, "ok": 0, "failed": 0, "budget_warned": False}


def reset_scrape_state() -> None:
    """Zero the per-run Amazon scraper counters (called by run_pipeline)."""
    _SCRAPE_STATE.update(used=0, ok=0, failed=0, budget_warned=False)


def max_scrapes() -> int:
    return int(os.getenv("MAX_SCRAPE", "10"))


def enrich_deal(deal: Deal, scrape: bool = False) -> Deal:
    """
    Enrich a deal with canonical image, URL, and price data. Called right
    after normalize_deal() in the pipeline.

    URL RESOLUTION (always on): deals that arrive as deal-site links
    (e.g. slickdeals.net interstitials) get their landing page fetched
    (the same single GET that feeds the og:image lookup). If the page
    carries a raw Amazon product href, deal.url is rewritten to the
    canonical amazon.com/dp/<ASIN> link — so CTAs and the affiliate tag
    point at Amazon, not at the interstitial. Landing pages whose only
    outbound link is a JS-protected click hop are resolved by the
    Playwright scraper below.

    PRICES, two modes:
      - scrape=True (CLI --scrape): deals whose URL points at Amazon —
        directly or via the click hop — get the REAL current/list price
        off the live product page (budget-capped at MAX_SCRAPE per run).
        Any failure (captcha, timeout, no price found) logs a WARNING and
        falls through to the mock logic below.
      - scrape=False (default): mock PA-API lookup —
          - no price at all        -> random mock price between $50 and $500
          - price, no original     -> mock original price 10-40% above it
        FUTURE: replace the mock with the real PA-API once approved.

    discount_percent is recalculated from whichever price pair is set
    (a real 0% discount stays 0% — no faking).

    IMAGES are always real: deals without an image get the og:image
    pulled off their landing page — Slickdeals' RSS summaries are plain
    text with no image data whatsoever.
    """
    # --- 1) One shared landing-page fetch: og:image + URL resolution. ---
    landing_html: Optional[str] = None
    if deal.url.startswith("http") and (
        not deal.image_url or not is_direct_amazon(deal.url)
    ):
        landing_html = fetch_landing_page(deal.url)
        time.sleep(0.1)  # be polite to the deal site
    if landing_html:
        if not deal.image_url:
            deal.image_url = extract_og_image(landing_html)
        if not is_direct_amazon(deal.url):
            resolved = extract_amazon_target(landing_html)
            if resolved:
                log.info(
                    "Resolved raw Amazon href: %s -> %s",
                    deal.url.split("?")[0][:70], resolved,
                )
                deal.url, deal.merchant = resolved, "amazon"

    # --- 2) Real Amazon prices (--scrape), budget-capped. ---
    scraped = False
    if scrape:
        budget_left = _SCRAPE_STATE["used"] < max_scrapes()
        asin_match = AMAZON_ASIN_RE.search(unquote(deal.url))
        if asin_match and budget_left:
            _SCRAPE_STATE["used"] += 1
            try:
                data = scrape_amazon_price(deal.url)
                deal.price = data["price"]
                deal.original_price = data["original_price"]
                if not deal.title.strip() and data.get("title"):
                    deal.title = data["title"]
                _SCRAPE_STATE["ok"] += 1
                log.info(
                    "Amazon scrape OK %s: price $%.2f (list %s)",
                    asin_match.group(1), data["price"],
                    f"${data['original_price']:.2f}" if data["original_price"] else "n/a",
                )
                scraped = True
            except AmazonBlockedError as exc:
                _SCRAPE_STATE["failed"] += 1
                log.warning(
                    "Amazon scrape failed for %s: %s; falling back to mock price",
                    deal.url, exc,
                )
            except Exception as exc:  # timeout, Playwright crash, ...
                _SCRAPE_STATE["failed"] += 1
                log.warning(
                    "Amazon scrape error for %s: %s; falling back to mock price",
                    deal.url, exc,
                )
        elif not asin_match and budget_left and landing_html:
            # Not a direct Amazon link yet — follow the deal site's click
            # hop in a real browser; it resolves AND scrapes in one visit.
            click_hop = extract_click_hop(landing_html)
            if click_hop:
                _SCRAPE_STATE["used"] += 1
                try:
                    data = scrape_amazon_price(click_hop=click_hop)
                    deal.url = data["resolved_url"] or deal.url
                    deal.merchant = "amazon"
                    deal.price = data["price"]
                    deal.original_price = data["original_price"]
                    if not deal.title.strip() and data.get("title"):
                        deal.title = data["title"]
                    _SCRAPE_STATE["ok"] += 1
                    log.info(
                        "Amazon scrape OK via click hop: %s price $%.2f (list %s)",
                        deal.url, data["price"],
                        f"${data['original_price']:.2f}" if data["original_price"] else "n/a",
                    )
                    scraped = True
                except AmazonBlockedError as exc:
                    _SCRAPE_STATE["failed"] += 1
                    log.warning(
                        "Click-hop scrape failed for %s: %s; falling back to mock price",
                        deal.url.split("?")[0][:70], exc,
                    )
                except Exception as exc:
                    _SCRAPE_STATE["failed"] += 1
                    log.warning(
                        "Click-hop scrape error for %s: %s; falling back to mock price",
                        deal.url.split("?")[0][:70], exc,
                    )
        elif not budget_left and not _SCRAPE_STATE["budget_warned"]:
            _SCRAPE_STATE["budget_warned"] = True
            log.warning(
                "Amazon scrape budget reached (MAX_SCRAPE=%d); "
                "remaining Amazon deals keep their mock prices",
                max_scrapes(),
            )

    # --- 3) Mock fallback + discount recalculation. ---
    if not scraped:
        if deal.price is None:
            deal.price = round(random.uniform(50, 500), 2)
        if deal.original_price is None and deal.price is not None:
            deal.original_price = round(deal.price * random.uniform(1.10, 1.40), 2)
    if deal.price is not None and deal.original_price \
            and deal.original_price > deal.price:
        deal.discount_percent = round(
            (1 - deal.price / deal.original_price) * 100, 1
        )
    else:
        deal.discount_percent = 0.0

    return deal


def score_deal(conn: sqlite3.Connection, deal: Deal) -> tuple[float, bool]:
    """
    Returns (score, qualifies).

    Qualifies when:
      - discount >= 20% OR
      - current price is the ALL-TIME low (full price_history — local
        observations plus any backfilled external history).

    score = discount_percent + (10 if all-time low else 0)

    Note: with a year of backfilled history, matching the all-time low is
    rare and meaningful; the score >= 15 filter (pipeline) keeps weak
    first sightings quiet.
    """
    if deal.price is None or deal.id is None:
        return 0.0, False

    row = conn.execute(
        "SELECT MIN(price) FROM price_history WHERE deal_id = ?",
        (deal.id,),
    ).fetchone()

    all_time_low = row[0] is not None and deal.price <= row[0] + 1e-9
    qualifies = deal.discount_percent >= 20 or all_time_low
    score = round(deal.discount_percent + (10 if all_time_low else 0), 1)
    return score, qualifies


# ---------------------------------------------------------------------------
# Affiliate links & alerting
# ---------------------------------------------------------------------------
def generate_affiliate_link(url: str, merchant: str) -> str:
    """
    Placeholder affiliate link generation — Amazon MVP edition.

    Resolution order:
      1. If a raw amazon.com/dp/ASIN (or /gp/product/ASIN) URL can be
         extracted from the link — either a direct Amazon URL or a target
         embedded in a redirector's query string (e.g. ?u=https%3A%2F%2F...
         amazon.com%2Fdp%2FB0XXXX) — return the canonical dp URL with the
         Associates tag appended.
      2. Else, if the URL is itself an Amazon link in some other format
         (or merchant == "Amazon"), append the tag in place (? or &).
      3. Else (e.g. Slickdeals deal links — they carry their own affiliate
         tags) return the URL untouched.

    Swap 'deal-finder-20' for your real Associates tag before going live.
    Real implementations for other networks would call/redirect through:
      - Awin:   https://www.awin1.com/cread.php?awinmid={MID}&awinaffid={AFFID}&ued={urlencode(url)}
      - CJ:     https://www.anrdoezrs.net/links/{PID}/type/dlg/{SID}/{urlencode(url)}
      - Amazon: the ?tag={ASSOCIATE_TAG} handled below (or the PA-API).
    """
    # 1) Raw Amazon product URL embedded in (or equal to) the link. The
    #    unquote() also catches percent-encoded redirect parameters.
    match = AMAZON_ASIN_RE.search(unquote(url))
    if match:
        asin = match.group(1).upper()
        return f"https://www.amazon.com/dp/{asin}?tag=deal-finder-20"

    # 2) Amazon link in another format: tag it in place.
    is_amazon = (
        (merchant or "").strip().lower() == "amazon" or "amazon.com" in url
    )
    if is_amazon and "tag=" not in url:
        separator = "&" if "?" in url else "?"
        return f"{url}{separator}tag=deal-finder-20"

    # 3) Not Amazon (Slickdeals links keep their own affiliate tags).
    return url


def format_deal_message(deal: Deal, score: float) -> str:
    """Build the HTML message sent to Telegram."""
    lines = [f"🔥 <b>{html.escape(deal.title)}</b>"]
    if deal.price is not None:
        price_line = f"💰 <b>${deal.price:,.2f}</b>"
        if deal.original_price:
            price_line += (
                f" <s>${deal.original_price:,.2f}</s>"
                f" (−{deal.discount_percent:.0f}%)"
            )
        lines.append(price_line)
    if deal.promo_code:
        lines.append(f"🏷 Promo code: <code>{html.escape(deal.promo_code)}</code>")
    lines.append(f"🏪 {html.escape(deal.merchant)} · via {html.escape(deal.source)}")
    link = generate_affiliate_link(deal.url, deal.merchant)
    lines.append(f'🔗 <a href="{html.escape(link, quote=True)}">Get the deal</a>')
    lines.append(f"📊 Score: {score}")
    return "\n".join(lines)


def already_alerted(conn: sqlite3.Connection, deal: Deal) -> bool:
    """
    True if this deal was already published at (roughly) this price.

    This gates the optional Telegram side-channel (and any future
    "NEW since last run" logic) — the HTML catalog always renders ALL
    currently-qualifying deals regardless of this flag.
    """
    if deal.id is None or deal.price is None:
        return False
    row = conn.execute(
        "SELECT last_alert_price FROM deals WHERE id = ?", (deal.id,)
    ).fetchone()
    return (
        row is not None
        and row[0] is not None
        and abs(row[0] - deal.price) < 0.01
    )


def mark_alerted(conn: sqlite3.Connection, deal: Deal) -> None:
    """Record that this deal was published at this price (gates the
    Telegram side-channel / future NEW-since-last-run logic)."""
    conn.execute(
        "UPDATE deals SET last_alert_price = ? WHERE id = ?",
        (deal.price, deal.id),
    )
    conn.commit()


def send_telegram(deal: Deal, score: float, dry_run: bool = False) -> bool:
    """Send one qualifying deal to the Telegram channel. Returns success."""
    message = format_deal_message(deal, score)

    if dry_run:
        # Strip HTML tags for a readable console preview.
        plain = re.sub(r"<[^>]+>", "", message)
        print(f"\n[DRY-RUN] Would send to Telegram:\n{html.unescape(plain)}\n")
        return True

    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        log.error("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID not configured; skipping send")
        return False

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return True
    except Exception as exc:
        log.error("Telegram send failed for %s: %s", deal.url, exc)
        return False


# ---------------------------------------------------------------------------
# Cross-source deduplication (rapidfuzz)
# ---------------------------------------------------------------------------
def _normalized_title(title: str) -> str:
    """Lowercase + strip punctuation/whitespace for fuzzy comparison."""
    return DEDUP_NOISE_RE.sub(" ", (title or "").lower()).strip()


def _prices_within_5pct(a: Optional[float], b: Optional[float]) -> bool:
    """True when both prices exist and differ by <= 5%. A missing price is
    never a duplicate (too risky to collapse on title alone)."""
    if not a or not b:
        return False
    hi, lo = max(a, b), min(a, b)
    return hi > 0 and (hi - lo) / hi <= 0.05


def _is_amazon_deal(deal: Deal) -> bool:
    return bool(AMAZON_ASIN_RE.search(unquote(deal.url or "")))


def deduplicate_deals(deals: list[Deal]) -> list[Deal]:
    """
    Collapse cross-source duplicates: normalized-title token_set_ratio >= 85
    AND prices within 5% -> keep ONE per cluster. Ranked first by "has an
    Amazon link" (deepest price history), then by pipeline score.
    """
    if len(deals) < 2:
        return deals
    if not RAPIDFUZZ_AVAILABLE:
        log.warning(
            "rapidfuzz not installed — skipping cross-source dedup "
            "(pip install rapidfuzz)"
        )
        return deals

    ranked = sorted(
        deals,
        key=lambda d: (_is_amazon_deal(d), d.score, d.discount_percent or 0),
        reverse=True,
    )
    kept: list[Deal] = []
    removed = 0
    for cand in ranked:
        dup_of = None
        for k in kept:
            ratio = fuzz.token_set_ratio(
                _normalized_title(cand.title), _normalized_title(k.title)
            )
            if ratio >= 85 and _prices_within_5pct(cand.price, k.price):
                dup_of = k
                break
        if dup_of is None:
            kept.append(cand)
        else:
            removed += 1
            log.info(
                "Dedup: dropped %r [%s] (duplicate of %r [%s])",
                cand.title[:50], cand.source, dup_of.title[:50], dup_of.source,
            )
    log.info(
        "Deduplicated: %d -> %d deals (%d cross-source duplicates removed)",
        len(deals), len(kept), removed,
    )
    return kept


# ---------------------------------------------------------------------------
# HTML rendering (primary output)
# ---------------------------------------------------------------------------
def render_html(
    deals: list[Deal],
    out_path: Path,
    new_window: timedelta = timedelta(hours=24),
    conn: Optional[sqlite3.Connection] = None,
    template_name: str = "deals.html",
) -> int:
    """
    Render the deal catalog to a static HTML page. Returns the number of
    deals written. This is the mandatory output of every pipeline run —
    even an empty qualifying list still produces an (empty-state) page so
    the auto-reload loop stays meaningful.

    With a DB connection each card also gets its price-history extras:
    the all-time low, a sparkline SVG, and the All-Time-Low badge flag.
    """
    now = datetime.now(timezone.utc)
    rows: list[dict] = []
    for deal in deals:
        # "NEW" = first seen within the new_window (default 24 h).
        is_new = False
        try:
            found = datetime.fromisoformat(deal.found_at)
            if found.tzinfo is None:  # be forgiving about naive timestamps
                found = found.replace(tzinfo=timezone.utc)
            is_new = (now - found) <= new_window
        except (TypeError, ValueError):
            is_new = False

        # All-time stats + sparkline from the full price history.
        sparkline, all_time_low, is_atl = "", None, False
        if conn is not None and deal.id is not None:
            points = get_price_history(conn, deal.id)
            if len(points) >= 2:
                all_time_low = min(p for _, p in points)
                sparkline = generate_sparkline(points)
                is_atl = (
                    deal.price is not None
                    and deal.price <= all_time_low + 1e-9
                )

        rows.append(
            {
                "title": deal.title,
                "url": deal.url,
                "affiliate_url": generate_affiliate_link(deal.url, deal.merchant),
                "price": deal.price,
                "original_price": deal.original_price,
                "discount_percent": deal.discount_percent,
                "promo_code": deal.promo_code,
                "image_url": deal.image_url,
                "is_new": is_new,
                "all_time_low": all_time_low,
                "sparkline": sparkline,
                "is_atl": is_atl,
                "category": deal.category,
            }
        )

    # Biggest discount first (None-safe sort key).
    rows.sort(
        key=lambda r: (r["discount_percent"] or 0), reverse=True
    )

    env = Environment(
        loader=FileSystemLoader("templates"),
        autoescape=select_autoescape(["html"]),
    )
    template = env.get_template(template_name)
    html_text = template.render(deals=rows, generated_at=now_iso())

    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Explicit UTF-8: never rely on the platform default (Windows cp1252).
    out_path.write_text(html_text, encoding="utf-8")

    log.info("Wrote %d deals to %s", len(rows), out_path)
    return len(rows)


def prune_old_deals(conn: sqlite3.Connection, days: int = 14) -> int:
    """
    Housekeeping: delete deals first seen more than `days` days ago so the
    DB stays small. Returns the number of rows pruned.
    """
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=days)
        ).isoformat(timespec="seconds")
        cur = conn.execute("DELETE FROM deals WHERE found_at < ?", (cutoff,))
        conn.commit()
        log.info("Pruned %d deals older than %d days", cur.rowcount, days)
        return cur.rowcount
    except Exception:
        log.exception("Prune of old deals failed")
        return 0


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def run_pipeline(dry_run: bool = False, scrape: bool = False) -> Optional[Path]:
    """
    One full pass: fetch -> normalize -> store -> score -> render HTML
    (+ optional Telegram side-channel). Returns the path of the HTML page
    that was written, or None if the pass crashed.

    scrape=True (CLI --scrape) replaces mock prices with real ones scraped
    off Amazon product pages (budget-capped by MAX_SCRAPE, default 10).
    """
    log.info("=== Deal finder run started ===")
    conn = get_db()
    reset_scrape_state()

    if scrape and not PLAYWRIGHT_AVAILABLE:
        log.warning(
            "--scrape requested but Playwright is not installed; "
            "using mock prices for everything"
        )
        scrape = False

    raw_items: list[tuple[dict, str]] = []

    # Sources: the 14-source registry (RSS + lightweight scrapes), falling
    # back to the mock affiliate feed only when EVERY source comes up dry.
    raw_items = fetch_all_sources()
    if not raw_items:
        log.info(
            "All sources yielded 0 items; falling back to the mock affiliate feed"
        )
        aff_path = os.getenv("AFFILIATE_FEED_FILE", "affiliate_feed_sample.json")
        raw_items = [(item, "affiliate") for item in fetch_affiliate(aff_path)]

    # Source 3: optional Playwright scrape of a JS-heavy page.
    scrape_url = (os.getenv("SCRAPE_SAMPLE_URL") or "").strip()
    if scrape_url:
        if PLAYWRIGHT_AVAILABLE:
            try:
                raw = scrape_sample(scrape_url)
                if raw:
                    raw_items.append((raw, "scraper"))
            except Exception as exc:
                log.error("Scrape failed for %s: %s", scrape_url, exc)
        else:
            log.warning("SCRAPE_SAMPLE_URL is set but Playwright is not installed; skipping")

    # Normalize (drop junk), then enrich (real Amazon prices via --scrape,
    # mock PA-API lookup otherwise), then store + score.
    deals: list[Deal] = []
    for raw, src in raw_items:
        deal = normalize_deal(raw, src)
        if deal is None:
            continue
        deal = enrich_deal(deal, scrape=scrape)
        deals.append(deal)
    log.info("Normalized + enriched %d deals from %d raw items", len(deals), len(raw_items))
    if scrape:
        log.info(
            "Amazon scraper: %d ok, %d failed (fell back to mock), budget %d/%d",
            _SCRAPE_STATE["ok"], _SCRAPE_STATE["failed"],
            _SCRAPE_STATE["used"], max_scrapes(),
        )

    # The HTML page is the user's browsable catalog: it ALWAYS renders all
    # currently-qualifying deals (qualifies AND score >= 15). last_alert_price
    # does NOT filter the page — it only gates the Telegram side-channel and
    # any future "NEW since last run" logic.
    qualifying: list[tuple[Deal, float]] = []
    for deal in deals:
        store_deal(conn, deal)
        backfill_external_history(conn, deal)  # year of history for new deals
        score, qualifies = score_deal(conn, deal)
        deal.score = score  # feeds the dedup ranking below
        if qualifies and score >= 15:
            qualifying.append((deal, score))

    # Cross-source duplicates collapse BEFORE rendering / alerting.
    qualifying_deals = deduplicate_deals([deal for deal, _ in qualifying])
    main_deals = [d for d in qualifying_deals if not d.promo_code]
    coupon_deals = [d for d in qualifying_deals if d.promo_code]
    log.info(
        "Qualifying after dedup: %d (%d deals page, %d coupons page)",
        len(qualifying_deals), len(main_deals), len(coupon_deals),
    )

    deals_page = Path("deals-preview.html" if dry_run else "deals.html")
    coupons_page = Path("coupons-preview.html" if dry_run else "coupons.html")
    if not qualifying_deals:
        log.info("No qualifying deals this run")
    # Always render — even an empty catalog keeps the pages' 30-minute
    # auto-reload loop meaningful.
    render_html(main_deals, deals_page, conn=conn, template_name="deals.html")
    render_html(coupon_deals, coupons_page, conn=conn, template_name="coupons.html")

    if not dry_run:
        # --- Optional Telegram side-channel: best-of-the-best only. ---
        telegram_ready = bool(os.getenv("TELEGRAM_BOT_TOKEN")) and bool(
            os.getenv("TELEGRAM_CHAT_ID")
        )
        if telegram_ready:
            for deal in qualifying_deals:
                if deal.score < 40:
                    continue
                if already_alerted(conn, deal):
                    continue  # already pushed at this price — no duplicates
                if send_telegram(deal, deal.score):
                    mark_alerted(conn, deal)
        else:
            log.info(
                "Telegram credentials not set; skipping Telegram side-channel "
                "(HTML page is the primary output)"
            )

        # Record publication for every qualifying deal. This gates the
        # Telegram side-channel and future "NEW since last run" logic; it
        # never filters what the HTML pages show.
        for deal in qualifying_deals:
            mark_alerted(conn, deal)

        # Housekeeping: drop stale rows from the DB.
        prune_old_deals(conn)

    log.info(
        "Run finished: %d deals processed, %d qualifying, pages: %s + %s",
        len(deals), len(qualifying_deals), deals_page, coupons_page,
    )
    conn.close()
    return deals_page


def main() -> None:
    # Windows consoles often default to cp1252, which chokes on the emojis
    # and en-dashes used in log/output lines. Force UTF-8 with replacement
    # so we never crash (stderr carries the WARNING lines too).
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Automated deal finder (MVP)")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Write deals-preview.html and do not mark alerts in the DB",
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Run a single pipeline pass and exit (cron-friendly)",
    )
    parser.add_argument(
        "--open", action="store_true",
        help="Open the generated HTML page in the default browser after writing",
    )
    parser.add_argument(
        "--interval", type=int, default=None, metavar="MINUTES",
        help="Minutes between runs in loop mode (default: env INTERVAL_MINUTES or 30)",
    )
    parser.add_argument(
        "--scrape", action="store_true",
        help="Fetch real Amazon prices with Playwright instead of mock prices "
             "(max MAX_SCRAPE deals per run, default 10)",
    )
    args = parser.parse_args()

    interval = args.interval or int(os.getenv("INTERVAL_MINUTES", "30"))

    while True:
        try:
            out_path = run_pipeline(dry_run=args.dry_run, scrape=args.scrape)
            if args.open and out_path is not None:
                webbrowser.open(out_path.resolve().as_uri())
        except Exception:
            # One bad pass should never kill the daemon — log and keep going.
            log.exception("Pipeline run failed")
        if args.once:
            break
        log.info("Sleeping %d minutes until next run...", interval)
        time.sleep(interval * 60)


if __name__ == "__main__":
    load_dotenv()  # read .env from the current directory
    main()
