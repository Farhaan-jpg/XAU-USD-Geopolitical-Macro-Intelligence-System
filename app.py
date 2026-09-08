"""
XAU/USD Geopolitical & Macro Intelligence System
Production-grade, low-latency event-driven algorithmic infrastructure and financial dashboard.
Includes OANDA Live Pricing Engine & Normalized Chronological Publication Sorter.
"""

import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

import aiosqlite
import feedparser
import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from liquidity_engine import (
    fetch_ohlcv_bars,
    LiquidityEngine,
    ZoneRadar,
    LiquidityProfileResult,
    ZoneAlertEvent,
    CandleBar,
    LiquidityZone,
    DEFAULT_LOOKBACK,
    DEFAULT_BINS,
    DEFAULT_PROXIMITY_BUFFER_USD,
)

# Load environment variables
load_dotenv()

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("XAUUSD_Engine")

# Configuration Constants
DB_PATH = os.getenv("DB_PATH", "intelligence.db")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "25"))
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
GROQ_MODEL = os.getenv("GROQ_MODEL", "").strip()

# Google Gemini Credentials & Models (Priority 1: Free Tier via AI Studio)
GEMINI_API_KEY = (os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "").strip()

# OpenRouter Credentials & Models (Priority 2: Permanent $0.00 free models)
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "").strip()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "").strip()

# Resilient Model Cascades for Automatic Fallbacks
# 1. Google Gemini Cascade (Priority 1)
GEMINI_CANDIDATE_MODELS = [m for m in [
    GEMINI_MODEL,
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemini-flash-lite-latest",
    "gemini-3.6-flash",
    "gemini-3.5-flash",
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-flash-latest",
] if m]

# 2. OpenRouter Cascade (Priority 2: Permanent Free Community Models)
OPENROUTER_CANDIDATE_MODELS = [m for m in [
    OPENROUTER_MODEL,
    "inclusionai/ling-3.0-flash-fin:free",
    "nvidia/nemotron-3.5-lightning:free",
    "liquid/lfm-2.5-2.6b:free",
    "poolside/laguna-s-2.1:free",
    "cohere/north-mini-code:free",
    "dots-studio/dots-3-note-preview:free",
    "inclusionai/ling-3.0-flash-sante:free",
    "google/gemma-4-26b-a4b-it:free",
    "google/gemma-4-31b-it:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "meta-llama/llama-3.1-8b-instruct:free",
] if m and m != "openrouter/auto"]

# 3. Groq Cascade (Priority 3: Ultra-low latency LPUs)
GROQ_CANDIDATE_MODELS = [m for m in [
    GROQ_MODEL,
    "qwen/qwen3.8-27b",
    "groq/compound-mini",
    "groq/compound",
    "openai/gpt-oss-20b",
    "openai/gpt-oss-120b",
    "qwen/qwen3.6-27b",
    "llama-3.1-8b-instant",
    "llama-3.3-70b-versatile",
    "allam-2-7b",
] if m]

# 4. OpenAI Cascade (Priority 4)
OPENAI_CANDIDATE_MODELS = [m for m in [
    OPENAI_MODEL,
    "gpt-4o-mini",
    "gpt-3.5-turbo",
    "gpt-4o",
] if m]

# Runtime working model cache
_active_gemini_model: Optional[str] = None
_active_openrouter_model: Optional[str] = None
_active_groq_model: Optional[str] = None
_active_openai_model: Optional[str] = None

# Provider-level Circuit Breakers & Cooldown State
_blacklisted_models: set = set()
_provider_cooldowns: Dict[str, float] = {
    "gemini": 0.0,
    "openrouter": 0.0,
    "groq": 0.0,
    "openai": 0.0,
}

def is_provider_cooling_down(provider: str) -> bool:
    """Checks if a provider is temporarily suspended due to 429 rate limit or quota exhaustion."""
    return time.time() < _provider_cooldowns.get(provider, 0.0)

def set_provider_cooldown(provider: str, duration_seconds: float = 600.0, reason: str = ""):
    """Trips provider circuit breaker to prevent cascading storms of failing calls across headlines."""
    _provider_cooldowns[provider] = time.time() + duration_seconds
    logger.warning(
        "[%s] Circuit breaker tripped (%s). Provider paused for %.0fs. Falling back to next provider...",
        provider.upper(), reason, duration_seconds
    )

# Telegram Notifications
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_NOTIFY_ALL_EVENTS = os.getenv("TELEGRAM_NOTIFY_ALL_EVENTS", "true").strip().lower() in ["true", "1", "yes"]

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

# OANDA Live Pricing Credentials
OANDA_API_KEY = os.getenv("OANDA_API_KEY", "").strip()
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "").strip()
OANDA_ENVIRONMENT = os.getenv("OANDA_ENVIRONMENT", "practice").strip().lower()

# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class AssetImpact(BaseModel):
    direction: str = Field(..., description="BULLISH | BEARISH | NEUTRAL")
    logic: str = Field(..., description="Brief economic correlation reasoning")

class CorrelatedAssets(BaseModel):
    DXY: AssetImpact
    US10Y_TIPS: AssetImpact
    WTI_Crude: AssetImpact
    Silver_XAG: AssetImpact
    VIX: AssetImpact

class IntelligenceEvent(BaseModel):
    id: str
    source: str
    title: str
    summary: str
    link: str
    published_at: str
    published_epoch: float = Field(default_factory=lambda: time.time())
    is_upcoming: bool = False
    is_simulated: bool = False
    relevance: bool = True
    severity: str = Field(..., description="CRITICAL | HIGH | MEDIUM | LOW")
    gold_bias: str = Field(..., description="STRONG_BULLISH | BULLISH | NEUTRAL | BEARISH | STRONG_BEARISH")
    potential_momentum: str = Field(..., description="15-40+ pips explosive | 5-15 pips drift | Muted/Noise")
    transmission_mechanism: str = Field(..., description="Exact 1-2 sentence economic reason")
    correlated_assets_impact: CorrelatedAssets
    created_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

class SimulateRequest(BaseModel):
    title: str
    summary: Optional[str] = ""
    source: Optional[str] = "Simulated Flash Wire"
    link: Optional[str] = "https://bloomberg.com/terminal/simulated"

# ---------------------------------------------------------------------------
# Database Management (aiosqlite WAL mode with Numeric Epoch Sorting)
# ---------------------------------------------------------------------------

async def init_db():
    """Initializes the SQLite database with WAL mode, schema, and epoch migration."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                source TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                link TEXT,
                published_at TEXT,
                published_epoch REAL,
                is_upcoming INTEGER DEFAULT 0,
                relevance INTEGER NOT NULL,
                severity TEXT NOT NULL,
                gold_bias TEXT NOT NULL,
                potential_momentum TEXT NOT NULL,
                transmission_mechanism TEXT NOT NULL,
                correlated_assets_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """)
        # Migration check: if column published_epoch or is_upcoming is missing in existing table
        try:
            await db.execute("ALTER TABLE events ADD COLUMN published_epoch REAL DEFAULT 0.0;")
        except Exception:
            pass  # Already exists
        try:
            await db.execute("ALTER TABLE events ADD COLUMN is_upcoming INTEGER DEFAULT 0;")
        except Exception:
            pass

        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_epoch ON events(published_epoch DESC);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_upcoming ON events(is_upcoming, published_epoch);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);")
        await db.commit()
    logger.info("SQLite persistence initialized at %s with WAL mode & epoch index.", DB_PATH)

async def store_event(event: IntelligenceEvent):
    """Persists a parsed intelligence event with numerical epoch timestamp and upcoming flag."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO events (
                id, source, title, summary, link, published_at, published_epoch, is_upcoming,
                relevance, severity, gold_bias, potential_momentum,
                transmission_mechanism, correlated_assets_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.id,
            event.source,
            event.title,
            event.summary,
            event.link,
            event.published_at,
            event.published_epoch,
            1 if event.is_upcoming else 0,
            1 if event.relevance else 0,
            event.severity,
            event.gold_bias,
            event.potential_momentum,
            event.transmission_mechanism,
            json.dumps(event.correlated_assets_impact.model_dump()),
            event.created_at,
        ))
        await db.commit()

async def get_recent_events(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Retrieves released/breaking events strictly ordered by published_epoch DESC, created_at DESC
    so the newest published news is ALWAYS on top!
    Excludes future scheduled events (which are served in upcoming calendar).
    """
    now = time.time()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT * FROM events 
               WHERE is_upcoming = 0 OR published_epoch <= ? 
               ORDER BY published_epoch DESC, created_at DESC 
               LIMIT ?""",
            (now + 60, limit)
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                item = dict(row)
                item["relevance"] = bool(item["relevance"])
                item["is_upcoming"] = bool(item.get("is_upcoming", 0))
                if not item.get("published_epoch"):
                    try:
                        item["published_epoch"] = datetime.fromisoformat(item["published_at"]).timestamp()
                    except Exception:
                        item["published_epoch"] = time.time()
                try:
                    item["correlated_assets_impact"] = json.loads(item["correlated_assets_json"])
                except Exception:
                    item["correlated_assets_impact"] = {}
                results.append(item)
            return results

async def get_upcoming_calendar(limit: int = 15) -> List[Dict[str, Any]]:
    """
    Retrieves upcoming scheduled economic releases ordered chronologically ascending (nearest event first).
    Enriches each release with actionable AI/Macro scenario triggers on Spot Gold (XAU/USD).
    """
    now = time.time()
    results = []
    seen_ids = set()

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                """SELECT * FROM events 
                   WHERE (is_upcoming = 1 OR published_epoch > ?) 
                   ORDER BY published_epoch ASC 
                   LIMIT ?""",
                (now, limit)
            ) as cursor:
                rows = await cursor.fetchall()
                for row in rows:
                    item = dict(row)
                    item["relevance"] = bool(item["relevance"])
                    item["is_upcoming"] = True
                    try:
                        item["correlated_assets_impact"] = json.loads(item["correlated_assets_json"])
                    except Exception:
                        item["correlated_assets_impact"] = {}
                    item["macro_scenarios"] = generate_macro_event_scenarios(
                        item.get("title", ""), item.get("summary", ""), item.get("source", "")
                    )
                    results.append(item)
                    seen_ids.add(item.get("id"))
    except Exception as e:
        logger.debug("Error querying upcoming events from DB: %s", e)

    # Supplement with any cached in-memory upcoming calendar releases
    for c_item in _calendar_cache.get("items", []):
        if c_item.get("id") not in seen_ids and float(c_item.get("published_epoch") or 0) > now:
            results.append(c_item)
            seen_ids.add(c_item.get("id"))

    results.sort(key=lambda x: float(x.get("published_epoch") or 0))
    return results[:limit]

async def is_event_processed(event_id: str) -> bool:
    """Checks if an event ID already exists in the database."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM events WHERE id = ?", (event_id,)) as cursor:
            row = await cursor.fetchone()
            return row is not None

# ---------------------------------------------------------------------------
# Telegram Bot Dispatcher & Macro Bias Aggregator
# ---------------------------------------------------------------------------

_telegram_sent_event_ids: Set[str] = set()

async def get_market_bias_stats() -> Dict[str, Any]:
    """
    Computes real-time overall macroeconomic sentiment bias and percentages
    across recently stored intelligence events in the SQLite database.
    """
    try:
        events = await get_recent_events(limit=50)
    except Exception as e:
        logger.debug("Error fetching recent events for market bias stats: %s", e)
        events = []

    total = len(events)
    if total == 0:
        return {
            "bias": "NEUTRAL",
            "emoji": "⚪",
            "bullish_pct": 50.0,
            "bearish_pct": 50.0,
            "neutral_pct": 0.0,
            "dominant_pct": 50.0,
            "total_events": 0,
            "summary_label": "NEUTRAL (50.0%)",
        }

    bullish = sum(1 for e in events if "BULLISH" in e.get("gold_bias", ""))
    bearish = sum(1 for e in events if "BEARISH" in e.get("gold_bias", ""))
    neutral = sum(1 for e in events if e.get("gold_bias") == "NEUTRAL")

    bullish_pct = round((bullish / total) * 100, 1)
    bearish_pct = round((bearish / total) * 100, 1)
    neutral_pct = round((neutral / total) * 100, 1)

    if bullish_pct >= 65.0:
        bias = "STRONG BULLISH"
        emoji = "🟢🟢"
        dominant_pct = bullish_pct
    elif bullish_pct >= 52.0:
        bias = "BULLISH"
        emoji = "🟢"
        dominant_pct = bullish_pct
    elif bearish_pct >= 65.0:
        bias = "STRONG BEARISH"
        emoji = "🔴🔴"
        dominant_pct = bearish_pct
    elif bearish_pct >= 52.0:
        bias = "BEARISH"
        emoji = "🔴"
        dominant_pct = bearish_pct
    else:
        bias = "NEUTRAL / BALANCED"
        emoji = "⚪"
        dominant_pct = max(bullish_pct, bearish_pct, neutral_pct)

    return {
        "bias": bias,
        "emoji": emoji,
        "bullish_pct": bullish_pct,
        "bearish_pct": bearish_pct,
        "neutral_pct": neutral_pct,
        "dominant_pct": dominant_pct,
        "total_events": total,
        "summary_label": f"{bias} ({dominant_pct}%)",
    }

def format_telegram_alert(event: IntelligenceEvent, market_stats: Optional[Dict[str, Any]] = None) -> str:
    """Formats event into a high-visibility, crisp HTML alert for Telegram with overall market bias & percentage."""
    bias_emoji = {
        "STRONG_BULLISH": "🟢🟢 <b>STRONG BULLISH</b>",
        "BULLISH": "🟢 <b>BULLISH</b>",
        "NEUTRAL": "⚪ <b>NEUTRAL</b>",
        "BEARISH": "🔴 <b>BEARISH</b>",
        "STRONG_BEARISH": "🔴🔴 <b>STRONG BEARISH</b>",
    }.get(event.gold_bias, event.gold_bias)

    severity_badge = {
        "CRITICAL": "🚨 [CRITICAL ALERT]",
        "HIGH": "🔥 [HIGH IMPACT]",
        "MEDIUM": "⚡ [MEDIUM IMPACT]",
        "LOW": "ℹ️ [LOW IMPACT]",
    }.get(event.severity, event.severity)

    corr = event.correlated_assets_impact
    dxy_dir = f"{'🟢' if corr.DXY.direction == 'BULLISH' else '🔴' if corr.DXY.direction == 'BEARISH' else '⚪'} {corr.DXY.direction}"
    tips_dir = f"{'🟢' if corr.US10Y_TIPS.direction == 'BULLISH' else '🔴' if corr.US10Y_TIPS.direction == 'BEARISH' else '⚪'} {corr.US10Y_TIPS.direction}"
    oil_dir = f"{'🟢' if corr.WTI_Crude.direction == 'BULLISH' else '🔴' if corr.WTI_Crude.direction == 'BEARISH' else '⚪'} {corr.WTI_Crude.direction}"
    silver_dir = f"{'🟢' if corr.Silver_XAG.direction == 'BULLISH' else '🔴' if corr.Silver_XAG.direction == 'BEARISH' else '⚪'} {corr.Silver_XAG.direction}"
    vix_dir = f"{'🟢' if corr.VIX.direction == 'BULLISH' else '🔴' if corr.VIX.direction == 'BEARISH' else '⚪'} {corr.VIX.direction}"

    # Overall Market Bias calculation & presentation
    if market_stats:
        m_bias = market_stats.get("bias", "NEUTRAL")
        m_emoji = market_stats.get("emoji", "⚪")
        m_dom = market_stats.get("dominant_pct", 50.0)
        m_bull = market_stats.get("bullish_pct", 50.0)
        m_bear = market_stats.get("bearish_pct", 50.0)
        m_neu = market_stats.get("neutral_pct", 0.0)
        m_total = market_stats.get("total_events", 0)
        market_bias_section = (
            f"🌐 <b>Overall Market Bias:</b> {m_emoji} <b>{m_bias} ({m_dom}%)</b>\n"
            f"📊 <i>Market Ratio: {m_bull}% Bullish | {m_bear}% Bearish | {m_neu}% Neutral ({m_total} events analyzed)</i>\n\n"
        )
    else:
        market_bias_section = ""

    safe_title = html.escape(event.title or "")
    safe_source = html.escape(event.source or "")
    safe_transmission = html.escape(event.transmission_mechanism or "")

    return (
        f"<b>{severity_badge}</b>\n"
        f"🏆 <b>Event Gold Bias:</b> {bias_emoji}\n"
        f"⚡ <b>Expected Momentum:</b> <code>{html.escape(event.potential_momentum or '')}</code>\n\n"
        f"{market_bias_section}"
        f"📰 <b>Headline:</b> {safe_title}\n"
        f"📡 <b>Source:</b> {safe_source}\n\n"
        f"🎯 <b>Transmission Mechanism:</b>\n"
        f"<i>{safe_transmission}</i>\n\n"
        f"📊 <b>Correlated Assets Breakdown:</b>\n"
        f"• <b>DXY:</b> {dxy_dir} ({html.escape(corr.DXY.logic or '')})\n"
        f"• <b>US10Y TIPS:</b> {tips_dir} ({html.escape(corr.US10Y_TIPS.logic or '')})\n"
        f"• <b>WTI Crude:</b> {oil_dir} ({html.escape(corr.WTI_Crude.logic or '')})\n"
        f"• <b>Silver (XAG):</b> {silver_dir} ({html.escape(corr.Silver_XAG.logic or '')})\n"
        f"• <b>VIX:</b> {vix_dir} ({html.escape(corr.VIX.logic or '')})\n\n"
        f"🔗 <a href='{event.link}'>View Original Source</a> | ⏱ <i>{event.published_at}</i>"
    )

async def dispatch_telegram_alert(event: IntelligenceEvent):
    """
    Dispatches alert to configured Telegram chat asynchronously.
    STRICT FILTER: ONLY sends current news and events (no historical backfill or stale news).
    Includes Overall Market Bias with exact percentage.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return

    # Deduplication guard to avoid repeat dispatches
    if event.id in _telegram_sent_event_ids:
        logger.debug("Telegram alert already sent for event %s, skipping.", event.id)
        return

    is_simulated = getattr(event, "is_simulated", False) or "Simulated" in event.source
    now = time.time()

    # 1. Guard against initial boot backfill spam:
    # If initial boot seeding is still running, suppress Telegram alerts for historical backlog.
    if not poller_state.initial_seed_completed and not is_simulated:
        logger.info("Telegram alert suppressed during initial boot seeding: %s", event.title[:45])
        return

    # 2. Strict Freshness Gate: ONLY current news and events!
    pub_epoch = float(event.published_epoch or 0.0)
    age_seconds = now - pub_epoch

    if not is_simulated:
        if event.is_upcoming:
            # For upcoming economic calendar events, only notify if occurring within the next 30 minutes
            time_until_event = pub_epoch - now
            if time_until_event < -600 or time_until_event > 1800:
                logger.info("Telegram alert skipped for upcoming calendar event outside immediate window (%.1f mins away): %s", time_until_event / 60, event.title[:45])
                return
        else:
            # For news and past events, strictly reject anything older than 20 minutes (1200 seconds)
            if age_seconds > 1200:
                logger.info("Telegram alert skipped for old news (age: %.1f mins): %s", age_seconds / 60, event.title[:45])
                return

    # 3. Severity filter if TELEGRAM_NOTIFY_ALL_EVENTS is False
    if not TELEGRAM_NOTIFY_ALL_EVENTS and event.severity not in ["CRITICAL", "HIGH"]:
        return

    # 4. Fetch real-time overall market bias & percentage
    stats = await get_market_bias_stats()
    text = format_telegram_alert(event, market_stats=stats)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                _telegram_sent_event_ids.add(event.id)
                logger.info("Telegram alert dispatched successfully for %s", event.id)
            else:
                resp_desc = resp.text
                try:
                    resp_desc = resp.json().get("description", resp.text)
                except Exception:
                    pass

                # If Telegram rejects HTML entities (HTTP 400), automatically retry plain text
                if resp.status_code == 400 and "can't parse entities" in resp_desc.lower():
                    logger.warning("Telegram rejected HTML entities for event %s, retrying plain text...", event.id)
                    plain_text = re.sub(r"<[^>]+>", "", text)
                    fallback_payload = {
                        "chat_id": TELEGRAM_CHAT_ID,
                        "text": plain_text,
                        "disable_web_page_preview": False,
                    }
                    retry_resp = await client.post(url, json=fallback_payload)
                    if retry_resp.status_code == 200:
                        _telegram_sent_event_ids.add(event.id)
                        logger.info("Telegram alert dispatched via plain-text fallback for %s", event.id)
                        return
                    else:
                        logger.warning("Telegram plain-text retry failed: %s", retry_resp.text)

                logger.warning("Telegram dispatch returned status %d: %s", resp.status_code, resp_desc)
    except Exception as e:
        logger.error("Error dispatching Telegram alert: %s", e)

# ---------------------------------------------------------------------------
# Dynamic Liquidity HeatMap Profile & Telegram Zone Alerts
# ---------------------------------------------------------------------------

liquidity_engine = LiquidityEngine(lookback=DEFAULT_LOOKBACK, bins=DEFAULT_BINS)
zone_radar = ZoneRadar(proximity_buffer_usd=DEFAULT_PROXIMITY_BUFFER_USD)
_latest_liquidity_profile: Optional[LiquidityProfileResult] = None
_recent_zone_alerts: List[Dict[str, Any]] = []
_liquidity_lock = asyncio.Lock()

def format_zone_telegram_alert(alert_event: ZoneAlertEvent, profile: Optional[LiquidityProfileResult] = None) -> str:
    """Formats a zone proximity/touch event into a high-visibility, professional Telegram alert."""
    zone = alert_event.zone
    is_entered = alert_event.event_type == "ENTERED"
    
    if is_entered:
        header_badge = "🎯 <b>[ZONE REACHED / TOUCHED] XAU/USD ALERT</b>"
        status_line = f"📍 <b>Price Action:</b> Price reached <b>${alert_event.current_price:.2f}</b> INSIDE the zone!"
    else:
        header_badge = "⚠️ <b>[PRICE NEARING ZONE] XAU/USD PROXIMITY</b>"
        status_line = f"📍 <b>Price Action:</b> Price is at <b>${alert_event.current_price:.2f}</b> (only <b>${alert_event.distance:.2f}</b> away from zone boundary)!"

    signal_badge = {
        "BUY": "🟢 <b>PRO CONTINUATION BUY / LONG</b>",
        "SELL": "🔴 <b>PRO CONTINUATION SELL / SHORT</b>",
        "REVERSAL_BUY": "🟠 <b>COUNTER-TREND REVERSAL (LONG BOUNCE)</b>",
        "REVERSAL_SELL": "🟠 <b>COUNTER-TREND REVERSAL (SHORT REJECTION)</b>",
        "RANGE_BUY": "🔵 <b>RANGE SUPPORT BOUNCE</b>",
        "RANGE_SELL": "🔵 <b>RANGE RESISTANCE REJECTION</b>",
    }.get(zone.signal, f"⚡ <b>{html.escape(zone.signal)}</b>")

    apex_tag = " 🌟 <b>[APEX ZONE - MAXIMUM LIQUIDITY]</b>" if zone.is_apex else ""
    zone_category = "MAJOR APEX/MACRO ZONE" if zone.is_major else "HIGH-QUALITY PULLBACK ZONE"

    trend_info = ""
    if profile:
        trend_label_safe = html.escape(profile.trend_label or "")
        trend_info = (
            f"• <b>Dual-MA Trend Context:</b> {trend_label_safe}\n"
            f"• <b>Fast EMA (50):</b> ${profile.fast_ma:.2f} | <b>Slow EMA (200):</b> ${profile.slow_ma:.2f}\n"
            f"• <b>Point of Control (POC):</b> ${profile.poc_price:.2f} (Peak Vol: {profile.poc_volume:.1f})\n"
            f"• <b>ATR(5) Volatility:</b> ${profile.atr:.2f}"
        )

    sl_direction = "below zone lower bound" if zone.is_lower else "above zone upper bound"
    safe_zone_type = html.escape(zone.zone_type or "")

    return (
        f"{header_badge}\n\n"
        f"🏆 <b>Symbol:</b> <code>XAU/USD (Spot Gold)</code>\n"
        f"🏷 <b>Zone Type:</b> <b>{safe_zone_type}</b>{apex_tag}\n"
        f"📊 <b>Category:</b> <code>{zone_category}</code>\n"
        f"🎯 <b>Actionable Signal:</b> {signal_badge}\n\n"
        f"{status_line}\n"
        f"📐 <b>Zone Range:</b> <code>${zone.lower:.2f} – ${zone.upper:.2f}</code> (Mid: <code>${zone.mid:.2f}</code>)\n"
        f"🛡 <b>Pine Script Stop Loss (SL):</b> <code>${zone.sl_price:.2f}</code> ({sl_direction})\n"
        f"💧 <b>Liquidity Concentration:</b> <code>{zone.volume_pct:.1f}%</code> of peak profile volume (Bin #{zone.bin_index})\n\n"
        f"🌐 <b>Market Condition Filters:</b>\n"
        f"{trend_info}\n\n"
        f"💡 <b>Tactical Advisory:</b> "
        f"<i>{'Watch for strong buyer defense and confirmation rejection wicks inside this support zone.' if zone.is_lower else 'Watch for seller exhaustion and rejection off this overhead resistance block.'}</i>\n"
        f"⏱ <i>{alert_event.timestamp}</i>"
    )

async def dispatch_zone_telegram_alert(alert_event: ZoneAlertEvent, profile: Optional[LiquidityProfileResult] = None) -> Dict[str, Any]:
    """
    Dispatches liquidity zone proximity/touch alert to Telegram chat.
    Records alert in memory history for dashboard and API tracking.
    Returns diagnostic dict with delivery status and Telegram API response.
    """
    text = format_zone_telegram_alert(alert_event, profile=profile)

    alert_record = {
        "event_type": alert_event.event_type,
        "zone_id": alert_event.zone.id,
        "zone_type": alert_event.zone.zone_type,
        "current_price": alert_event.current_price,
        "distance": alert_event.distance,
        "range": f"${alert_event.zone.lower:.2f} - ${alert_event.zone.upper:.2f}",
        "sl_price": alert_event.zone.sl_price,
        "signal": alert_event.zone.signal,
        "timestamp": alert_event.timestamp,
        "message": alert_event.message,
    }
    _recent_zone_alerts.insert(0, alert_record)
    if len(_recent_zone_alerts) > 100:
        _recent_zone_alerts.pop()

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("[MOCK TELEGRAM] Zone Alert Triggered (TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not configured):\n%s", alert_event.message)
        return {
            "success": False,
            "status": "not_configured",
            "detail": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID environment variable is missing on this server instance.",
            "http_code": None,
        }

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info("Telegram Zone Alert dispatched successfully: %s", alert_event.message[:60])
                return {
                    "success": True,
                    "status": "delivered",
                    "detail": "Alert delivered successfully to Telegram chat.",
                    "http_code": 200,
                }
            
            resp_desc = resp.text
            try:
                resp_desc = resp.json().get("description", resp.text)
            except Exception:
                pass

            # If Telegram rejects HTML entities (HTTP 400), automatically retry plain text fallback
            if resp.status_code == 400 and "can't parse entities" in resp_desc.lower():
                logger.warning("Telegram rejected HTML entities, retrying with plain text fallback...")
                plain_text = re.sub(r"<[^>]+>", "", text)
                fallback_payload = {
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": plain_text,
                    "disable_web_page_preview": True,
                }
                retry_resp = await client.post(url, json=fallback_payload)
                if retry_resp.status_code == 200:
                    logger.info("Telegram Zone Alert delivered via plain-text fallback: %s", alert_event.message[:60])
                    return {
                        "success": True,
                        "status": "delivered",
                        "detail": "Alert delivered successfully to Telegram (via plain-text fallback).",
                        "http_code": 200,
                    }
                else:
                    try:
                        retry_desc = retry_resp.json().get("description", retry_resp.text)
                    except Exception:
                        retry_desc = retry_resp.text
                    return {
                        "success": False,
                        "status": "failed",
                        "detail": f"Telegram API error {retry_resp.status_code}: {retry_desc}",
                        "http_code": retry_resp.status_code,
                    }

            logger.warning("Telegram Zone dispatch returned status %d: %s", resp.status_code, resp_desc)
            return {
                "success": False,
                "status": "failed",
                "detail": f"Telegram API error {resp.status_code}: {resp_desc}",
                "http_code": resp.status_code,
            }
    except Exception as e:
        logger.error("Error dispatching Telegram Zone alert: %s", e)
        return {
            "success": False,
            "status": "failed",
            "detail": f"Network exception: {str(e)}",
            "http_code": None,
        }

def serialize_liquidity_profile(profile: LiquidityProfileResult) -> Dict[str, Any]:
    """Helper to convert LiquidityProfileResult to JSON-serializable dictionary."""
    return {
        "timestamp": profile.timestamp,
        "symbol": profile.symbol,
        "current_price": profile.current_price,
        "lookback": profile.lookback,
        "bins_count": profile.bins_count,
        "trend_state": profile.trend_state,
        "trend_label": profile.trend_label,
        "fast_ma": profile.fast_ma,
        "slow_ma": profile.slow_ma,
        "atr": profile.atr,
        "top_boundary": profile.top_boundary,
        "bot_boundary": profile.bot_boundary,
        "poc_price": profile.poc_price,
        "poc_volume": profile.poc_volume,
        "apex_zone": vars(profile.apex_zone) if profile.apex_zone else None,
        "major_zones": [vars(z) for z in profile.major_zones],
        "pullback_zones": [vars(z) for z in profile.pullback_zones],
        "all_active_zones": [vars(z) for z in profile.all_active_zones],
        "heatmap_bins": [vars(b) for b in profile.heatmap_bins],
        "nearest_support": vars(profile.nearest_support) if profile.nearest_support else None,
        "nearest_resistance": vars(profile.nearest_resistance) if profile.nearest_resistance else None,
        "distance_to_nearest_support": profile.distance_to_nearest_support,
        "distance_to_nearest_resistance": profile.distance_to_nearest_resistance,
    }

DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5m").strip().lower()
_current_timeframe: str = DEFAULT_TIMEFRAME

async def refresh_liquidity_profile(timeframe: Optional[str] = None):
    """Fetches latest candle bars and recomputes the 300-bar liquidity heatmap profile."""
    global _latest_liquidity_profile, _current_timeframe
    tf = (timeframe or _current_timeframe).strip().lower()
    _current_timeframe = tf
    try:
        current_price = poller_state.current_gold_price
        bars = await fetch_ohlcv_bars(
            lookback=DEFAULT_LOOKBACK,
            timeframe=tf,
            oanda_api_key=OANDA_API_KEY,
            oanda_account_id=OANDA_ACCOUNT_ID,
            oanda_env=OANDA_ENVIRONMENT,
            current_spot_price=current_price,
        )
        if bars:
            async with _liquidity_lock:
                profile = liquidity_engine.compute(bars, current_spot_price=current_price)
                _latest_liquidity_profile = profile
            logger.info("Liquidity HeatMap Profile updated [%s]: POC=$%.2f, Zones=%d, Trend=%s",
                        tf.upper(), profile.poc_price, len(profile.all_active_zones), profile.trend_label)
            serialized = serialize_liquidity_profile(profile)
            serialized["timeframe"] = tf
            await hub.broadcast("liquidity_profile", serialized)
    except Exception as e:
        logger.warning("Error refreshing liquidity profile (%s): %s", tf, e)

async def periodic_liquidity_profile_updater_task():
    """Background task refreshing the liquidity heatmap profile every 60 seconds."""
    await asyncio.sleep(2.0)
    while poller_state.poller_active:
        try:
            await refresh_liquidity_profile()
            await asyncio.sleep(60.0)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug("Error in liquidity profile periodic updater: %s", e)
            await asyncio.sleep(15.0)

# ---------------------------------------------------------------------------
# Quantitative LLM Parsing Engine
# ---------------------------------------------------------------------------

LLM_SYSTEM_PROMPT = r"""
You are a Senior Chief Macro Strategist & Quantitative Gold (XAU/USD) Portfolio Manager.
Your role is to analyze incoming geopolitical headlines, macroeconomic indicators, and central bank press releases to produce high-precision trading intelligence for spot Gold (XAU/USD).

Gold Pricing Fundamentals:
1. Real Interest Rates ($r = y - \pi$): Gold is non-yielding. Inverse correlation to US 10Y TIPS real yields is ~ -0.85. Higher yields = Gold Bearish.
2. US Dollar Index (DXY): Gold is priced in USD. Inverse correlation ~ -0.80. Stronger USD = Gold Bearish.
3. Geopolitical / Safe Haven Risk Premia: Sudden military strikes, blockades, wars trigger safe haven flight into Gold regardless of yields.
4. Energy & Inflation Pass-through: Crude oil spikes increase CPI expectations, driving inflation hedge demand for bullion.
5. Monetary Metal Beta: Silver (XAG/USD) moves with ~1.5x-2x beta to Gold.

You MUST reply with ONLY valid JSON matching this schema:
{
  "relevance": true,
  "severity": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW",
  "gold_bias": "STRONG_BULLISH" | "BULLISH" | "NEUTRAL" | "BEARISH" | "STRONG_BEARISH",
  "potential_momentum": "15-40+ pips explosive" | "5-15 pips drift" | "Muted/Noise",
  "transmission_mechanism": "Exact 1-2 sentence economic/macro reason explaining the mechanical transmission to Gold.",
  "correlated_assets_impact": {
    "DXY": {"direction": "BULLISH" | "BEARISH" | "NEUTRAL", "logic": "one short phrase"},
    "US10Y_TIPS": {"direction": "BULLISH" | "BEARISH" | "NEUTRAL", "logic": "one short phrase"},
    "WTI_Crude": {"direction": "BULLISH" | "BEARISH" | "NEUTRAL", "logic": "one short phrase"},
    "Silver_XAG": {"direction": "BULLISH" | "BEARISH" | "NEUTRAL", "logic": "one short phrase"},
    "VIX": {"direction": "BULLISH" | "BEARISH" | "NEUTRAL", "logic": "one short phrase"}
  }
}
If the headline has no macro or geopolitical relevance to global financial markets or Gold, set "relevance": false and severity: "LOW".
"""

def heuristic_quant_analysis(title: str, summary: str, source: str) -> Dict[str, Any]:
    """
    Built-in high-accuracy deterministic rule-based quantitative parser.
    Used when no external LLM API key is configured or as an instantaneous resilient fallback.
    """
    text = (f"{title} {summary}").lower()

    war_keywords = [
        "strike", "missile", "attack", "iran", "israel", "gaza", "strait of hormuz",
        "red sea", "taiwan", "russia", "ukraine", "nuclear", "war", "military",
        "explosion", "sanction", "drone", "conflict", "clashes", "retaliation"
    ]
    bullish_macro = [
        "rate cut", "dovish", "inflation falls", "cpi drops", "cpi lower", "cooling",
        "unemployment rises", "fed pauses", "yields drop", "easing", "pce drops",
        "recession fears", "slowdown", "weak dollar"
    ]
    bearish_macro = [
        "rate hike", "hawkish", "inflation surges", "cpi jumps", "cpi higher", "hot cpi",
        "strong jobs", "nfp surges", "yields surge", "tightening", "powell hawkish",
        "strong dollar", "robust growth"
    ]
    macro_relevance = [
        "fomc", "federal reserve", "powell", "cpi", "ppi", "nfp", "gdp", "pce",
        "interest rate", "treasury", "central bank", "ecb", "boj", "inflation"
    ]

    has_war = any(k in text for k in war_keywords)
    has_bullish_macro = any(k in text for k in bullish_macro)
    has_bearish_macro = any(k in text for k in bearish_macro)
    has_macro = any(k in text for k in macro_relevance) or "federal reserve" in source.lower()

    if has_war:
        severity = "CRITICAL" if any(k in text for k in ["missile", "nuclear", "iran", "strait of hormuz", "strike"]) else "HIGH"
        return {
            "relevance": True,
            "severity": severity,
            "gold_bias": "STRONG_BULLISH",
            "potential_momentum": "15-40+ pips explosive",
            "transmission_mechanism": "Immediate safe-haven flight into bullion triggered by geopolitical risk premia and potential energy supply disruption fears.",
            "correlated_assets_impact": {
                "DXY": {"direction": "BULLISH", "logic": "Global liquidity haven bid alongside gold."},
                "US10Y_TIPS": {"direction": "BEARISH", "logic": "Bond buying compresses nominal benchmark yields."},
                "WTI_Crude": {"direction": "BULLISH", "logic": "Geopolitical supply disruption premium."},
                "Silver_XAG": {"direction": "STRONG_BULLISH" if "STRONG" in severity else "BULLISH", "logic": "Sympathetic precious metals beta rally."},
                "VIX": {"direction": "BULLISH", "logic": "Equity volatility and tail-risk hedging surge."}
            }
        }
    elif has_bullish_macro:
        return {
            "relevance": True,
            "severity": "HIGH",
            "gold_bias": "STRONG_BULLISH",
            "potential_momentum": "15-40+ pips explosive",
            "transmission_mechanism": "Soft macro prints lower terminal Fed expectations, compressing US 10Y real yields ($r = y - \\pi$) and removing the opportunity cost of holding non-yielding bullion.",
            "correlated_assets_impact": {
                "DXY": {"direction": "BEARISH", "logic": "Dollar weakness on dovish interest rate repricing."},
                "US10Y_TIPS": {"direction": "BEARISH", "logic": "Real discount rates plunge across the curve."},
                "WTI_Crude": {"direction": "NEUTRAL", "logic": "Offsetting growth concerns vs USD debasement."},
                "Silver_XAG": {"direction": "BULLISH", "logic": "Industrial & monetary metal tailwind."},
                "VIX": {"direction": "BEARISH", "logic": "Financial conditions ease."}
            }
        }
    elif has_bearish_macro:
        return {
            "relevance": True,
            "severity": "HIGH",
            "gold_bias": "STRONG_BEARISH",
            "potential_momentum": "15-40+ pips explosive",
            "transmission_mechanism": "Hot macroeconomic data solidifies higher-for-longer Fed policy, driving real yields higher and strengthening the USD, which triggers aggressive institutional selling in bullion.",
            "correlated_assets_impact": {
                "DXY": {"direction": "BULLISH", "logic": "Rate differentials attract capital inflows into USD."},
                "US10Y_TIPS": {"direction": "BULLISH", "logic": "Benchmark real yields surge, raising opportunity cost."},
                "WTI_Crude": {"direction": "BEARISH", "logic": "Fears of demand destruction from tighter credit."},
                "Silver_XAG": {"direction": "BEARISH", "logic": "Precious metals dump on elevated real rate hurdles."},
                "VIX": {"direction": "BULLISH", "logic": "Rate volatility spills into equity valuations."}
            }
        }
    elif has_macro:
        return {
            "relevance": True,
            "severity": "MEDIUM",
            "gold_bias": "NEUTRAL",
            "potential_momentum": "5-15 pips drift",
            "transmission_mechanism": "Baseline central bank communication and macroeconomic updates keeping gold in a rotational consolidation pending yield breakout.",
            "correlated_assets_impact": {
                "DXY": {"direction": "NEUTRAL", "logic": "Awaiting definitive data catalysts."},
                "US10Y_TIPS": {"direction": "NEUTRAL", "logic": "Yields rangebound across key moving averages."},
                "WTI_Crude": {"direction": "NEUTRAL", "logic": "Stable inventory and demand dynamics."},
                "Silver_XAG": {"direction": "NEUTRAL", "logic": "Consolidating near support/resistance."},
                "VIX": {"direction": "NEUTRAL", "logic": "Implied volatility flat."}
            }
        }
    else:
        is_relevant = any(w in text for w in ["economy", "trade", "tariff", "oil", "opec", "debt", "bank"])
        return {
            "relevance": is_relevant,
            "severity": "LOW",
            "gold_bias": "NEUTRAL",
            "potential_momentum": "Muted/Noise",
            "transmission_mechanism": "Secondary economic or trade noise with marginal immediate transmission to benchmark XAU/USD real interest rate channels.",
            "correlated_assets_impact": {
                "DXY": {"direction": "NEUTRAL", "logic": "Indeterminate directional bias."},
                "US10Y_TIPS": {"direction": "NEUTRAL", "logic": "No direct reaction in real curve."},
                "WTI_Crude": {"direction": "NEUTRAL", "logic": "Trading on technicals."},
                "Silver_XAG": {"direction": "NEUTRAL", "logic": "Tracking gold consolidations."},
                "VIX": {"direction": "NEUTRAL", "logic": "Subdued risk sentiment."}
            }
        }

def _clean_llm_json(raw_text: str) -> Optional[Dict[str, Any]]:
    """Safely cleans and parses JSON from any LLM response, stripping markdown fences or preamble."""
    if not raw_text:
        return None
    text = raw_text.strip()
    if "```" in text:
        match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
        if match:
            text = match.group(1).strip()
    if not text.startswith("{"):
        start_idx = text.find("{")
        end_idx = text.rfind("}")
        if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
            text = text[start_idx:end_idx + 1]
    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return None

async def call_gemini_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls Google Gemini API (Priority 1) with automatic cascading fallback across candidate models."""
    global _active_gemini_model
    if not GEMINI_API_KEY or is_provider_cooling_down("gemini"):
        return None

    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."
    prompt_text = f"{LLM_SYSTEM_PROMPT}\n\nTask:\n{user_content}"

    models_to_try = [_active_gemini_model] if _active_gemini_model and _active_gemini_model not in _blacklisted_models else []
    for m in GEMINI_CANDIDATE_MODELS:
        if m not in models_to_try and m not in _blacklisted_models:
            models_to_try.append(m)

    if not models_to_try:
        return None

    for model_name in models_to_try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent?key={GEMINI_API_KEY}"
        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": {
                "response_mime_type": "application/json",
                "temperature": 0.1,
                "max_output_tokens": 600,
            }
        }

        try:
            async with httpx.AsyncClient(timeout=7.0) as client:
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    candidates = data.get("candidates", [])
                    if candidates and "content" in candidates[0]:
                        parts = candidates[0]["content"].get("parts", [])
                        if parts and "text" in parts[0]:
                            parsed = _clean_llm_json(parts[0]["text"])
                            if parsed and "relevance" in parsed:
                                if _active_gemini_model != model_name:
                                    logger.info("Google Gemini model active & verified: %s", model_name)
                                    _active_gemini_model = model_name
                                return parsed
                elif resp.status_code == 429:
                    if _active_gemini_model == model_name:
                        _active_gemini_model = None
                    set_provider_cooldown("gemini", duration_seconds=600.0, reason="Quota Exceeded (429)")
                    return None
                elif resp.status_code in [404, 400]:
                    _blacklisted_models.add(model_name)
                    if _active_gemini_model == model_name:
                        _active_gemini_model = None
                    logger.warning("Google Gemini model '%s' unavailable (status %d). Blacklisted and switching to fallback...", model_name, resp.status_code)
                    continue
                else:
                    logger.warning("Google Gemini API returned status %d with model %s: %s", resp.status_code, model_name, resp.text[:120])
        except (httpx.TimeoutException, httpx.ReadTimeout):
            logger.warning("Google Gemini model '%s' timed out. Blacklisting for this session.", model_name)
            _blacklisted_models.add(model_name)
            if _active_gemini_model == model_name:
                _active_gemini_model = None
            continue
        except Exception as e:
            logger.error("Google Gemini API call error with model %s: %s", model_name, e)
            continue

    return None

async def call_openrouter_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls OpenRouter API (Priority 2) with automatic cascading fallback across free candidate models."""
    global _active_openrouter_model
    if not OPENROUTER_API_KEY or is_provider_cooling_down("openrouter"):
        return None

    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "HTTP-Referer": "https://github.com/Farhaan-jpg/XAU-USD-Geopolitical-Macro-Intelligence-System",
        "X-Title": "XAUUSD Macro Intelligence System",
    }
    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."

    models_to_try = [_active_openrouter_model] if _active_openrouter_model and _active_openrouter_model not in _blacklisted_models else []
    for m in OPENROUTER_CANDIDATE_MODELS:
        if m not in models_to_try and m not in _blacklisted_models:
            models_to_try.append(m)

    if not models_to_try:
        return None

    for model_name in models_to_try:
        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.1,
            "max_tokens": 600,
        }

        try:
            async with httpx.AsyncClient(timeout=7.5) as client:
                resp = await client.post(url, headers=headers, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices and "message" in choices[0]:
                        raw_json = choices[0]["message"].get("content", "")
                        parsed = _clean_llm_json(raw_json)
                        if parsed and "relevance" in parsed:
                            if _active_openrouter_model != model_name:
                                logger.info("OpenRouter model active & verified: %s", model_name)
                                _active_openrouter_model = model_name
                            return parsed
                elif resp.status_code == 429:
                    err_text = resp.text
                    if _active_openrouter_model == model_name:
                        _active_openrouter_model = None
                    if "free-models-per-day" in err_text or "limit" in err_text.lower():
                        set_provider_cooldown("openrouter", duration_seconds=600.0, reason="Daily Free Limit Exceeded (429)")
                        return None
                    logger.warning("OpenRouter model '%s' rate limited (status 429). Switching to fallback...", model_name)
                    continue
                elif resp.status_code == 402:
                    _blacklisted_models.add(model_name)
                    if _active_openrouter_model == model_name:
                        _active_openrouter_model = None
                    logger.warning("OpenRouter model '%s' returned status 402 (Payment Required / Credits needed). Blacklisting model.", model_name)
                    continue
                elif resp.status_code in [403, 404]:
                    _blacklisted_models.add(model_name)
                    if _active_openrouter_model == model_name:
                        _active_openrouter_model = None
                    logger.warning("OpenRouter model '%s' unavailable (status %d). Blacklisted and switching to fallback...", model_name, resp.status_code)
                    continue
                else:
                    logger.warning("OpenRouter API returned status %d with model %s: %s", resp.status_code, model_name, resp.text[:120])
        except (httpx.TimeoutException, httpx.ReadTimeout):
            logger.warning("OpenRouter model '%s' timed out. Blacklisting for this session.", model_name)
            _blacklisted_models.add(model_name)
            if _active_openrouter_model == model_name:
                _active_openrouter_model = None
            continue
        except Exception as e:
            logger.error("OpenRouter API call error with model %s: %s", model_name, e)
            continue

    set_provider_cooldown("openrouter", duration_seconds=600.0, reason="All OpenRouter models failed")
    return None

async def call_groq_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls Groq API (Priority 3) with automatic cascading fallback across candidate models."""
    global _active_groq_model
    if not GROQ_API_KEY or is_provider_cooling_down("groq"):
        return None

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    }
    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."

    models_to_try = [_active_groq_model] if _active_groq_model and _active_groq_model not in _blacklisted_models else []
    for m in GROQ_CANDIDATE_MODELS:
        if m not in models_to_try and m not in _blacklisted_models:
            models_to_try.append(m)

    if not models_to_try:
        return None

    for model_name in models_to_try:
        # First attempt with json_object mode; if 400 json_validate_failed retry without json_object mode
        for use_json_format in [True, False]:
            payload = {
                "model": model_name,
                "messages": [
                    {"role": "system", "content": LLM_SYSTEM_PROMPT},
                    {"role": "user", "content": user_content},
                ],
                "temperature": 0.1,
                "max_tokens": 600,
            }
            if use_json_format:
                payload["response_format"] = {"type": "json_object"}

            try:
                async with httpx.AsyncClient(timeout=7.0) as client:
                    resp = await client.post(url, headers=headers, json=payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        choices = data.get("choices", [])
                        if choices and "message" in choices[0]:
                            raw_json = choices[0]["message"].get("content", "")
                            parsed = _clean_llm_json(raw_json)
                            if parsed and "relevance" in parsed:
                                if _active_groq_model != model_name:
                                    logger.info("Groq model active & verified: %s", model_name)
                                    _active_groq_model = model_name
                                return parsed
                    elif resp.status_code == 400 and use_json_format:
                        # json_validate_failed: try next loop iteration without response_format
                        continue
                    elif resp.status_code == 429:
                        if _active_groq_model == model_name:
                            _active_groq_model = None
                        logger.warning("Groq model '%s' rate limited (status 429). Switching to fallback...", model_name)
                        break
                    elif resp.status_code in [404, 400]:
                        _blacklisted_models.add(model_name)
                        if _active_groq_model == model_name:
                            _active_groq_model = None
                        logger.warning("Groq model '%s' unavailable (status %d). Blacklisted and switching to fallback...", model_name, resp.status_code)
                        break
                    else:
                        logger.warning("Groq API returned status %d with model %s: %s", resp.status_code, model_name, resp.text[:120])
                        break
            except (httpx.TimeoutException, httpx.ReadTimeout):
                logger.warning("Groq model '%s' timed out. Blacklisting for this session.", model_name)
                _blacklisted_models.add(model_name)
                if _active_groq_model == model_name:
                    _active_groq_model = None
                break
            except Exception as e:
                logger.error("Groq API call error with model %s: %s", model_name, e)
                break

    set_provider_cooldown("groq", duration_seconds=300.0, reason="All Groq models failed")
    return None

async def call_openai_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls OpenAI API (Priority 4) with automatic cascading fallback across candidate models."""
    global _active_openai_model
    if not OPENAI_API_KEY or is_provider_cooling_down("openai"):
        return None

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    }
    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."

    models_to_try = [_active_openai_model] if _active_openai_model and _active_openai_model not in _blacklisted_models else []
    for m in OPENAI_CANDIDATE_MODELS:
        if m not in models_to_try and m not in _blacklisted_models:
            models_to_try.append(m)

    if not models_to_try:
        return None

    for model_name in models_to_try:
        payload = {
            "model": model_name,
            "messages": [
                {"role": "system", "content": LLM_SYSTEM_PROMPT},
                {"role": "user", "content": user_content},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.1,
            "max_tokens": 600,
        }

        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    choices = data.get("choices", [])
                    if choices and "message" in choices[0]:
                        raw_json = choices[0]["message"].get("content", "")
                        parsed = _clean_llm_json(raw_json)
                        if parsed and "relevance" in parsed:
                            if _active_openai_model != model_name:
                                logger.info("OpenAI model active & verified: %s", model_name)
                                _active_openai_model = model_name
                            return parsed
                elif resp.status_code == 429:
                    set_provider_cooldown("openai", duration_seconds=600.0, reason="Rate Limited (429)")
                    return None
                elif resp.status_code in [404, 400]:
                    _blacklisted_models.add(model_name)
                    if _active_openai_model == model_name:
                        _active_openai_model = None
                    logger.warning("OpenAI model '%s' unavailable (status %d). Blacklisted and switching to fallback...", model_name, resp.status_code)
                    continue
                else:
                    logger.warning("OpenAI API returned status %d with model %s: %s", resp.status_code, model_name, resp.text[:120])
        except (httpx.TimeoutException, httpx.ReadTimeout):
            logger.warning("OpenAI model '%s' timed out. Blacklisting for this session.", model_name)
            _blacklisted_models.add(model_name)
            if _active_openai_model == model_name:
                _active_openai_model = None
            continue
        except Exception as e:
            logger.error("OpenAI API call error with model %s: %s", model_name, e)
            continue

    return None

async def auto_select_working_ai_models():
    """Proactively discovers and caches the best running, available models from Google, OpenRouter, and Groq in real time,
    with active probe health checks to trip circuit breakers before traffic starts."""
    global _active_gemini_model, _active_openrouter_model, _active_groq_model, _active_openai_model
    global GEMINI_CANDIDATE_MODELS, OPENROUTER_CANDIDATE_MODELS, GROQ_CANDIDATE_MODELS

    probe_prompt = "Respond with JSON: {\"status\": \"ok\"}"

    # 1. Google Gemini Real-Time Discovery & Health Check (Priority 1)
    if GEMINI_API_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models?key={GEMINI_API_KEY}"
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    discovered_gemini = [
                        m.get("name", "").replace("models/", "")
                        for m in data.get("models", [])
                        if "generateContent" in m.get("supportedGenerationMethods", [])
                        and any(sub in m.get("name", "") for sub in ["flash", "gemma", "pro"])
                        and "tts" not in m.get("name", "")
                        and "image" not in m.get("name", "")
                        and "preview-1" not in m.get("name", "")
                    ]
                    if discovered_gemini:
                        pref_gemini = [
                            "gemini-3.5-flash-lite", "gemini-3.1-flash-lite", "gemini-flash-lite-latest",
                            "gemini-3.6-flash", "gemini-3.5-flash", "gemini-3.8-flash", "gemini-3.7-flash", "gemini-flash-latest"
                        ]
                        ordered_gemini = [m for m in pref_gemini if m in discovered_gemini]
                        for m in discovered_gemini:
                            if m not in ordered_gemini:
                                ordered_gemini.append(m)
                        GEMINI_CANDIDATE_MODELS = ordered_gemini
                        _active_gemini_model = GEMINI_CANDIDATE_MODELS[0]
        except Exception as e:
            logger.warning("Could not fetch real-time Gemini models: %s. Using candidate cascade.", e)
        if not _active_gemini_model and GEMINI_CANDIDATE_MODELS:
            _active_gemini_model = GEMINI_CANDIDATE_MODELS[0]

        # Active Health Probe for Gemini
        if _active_gemini_model:
            try:
                probe_url = f"https://generativelanguage.googleapis.com/v1beta/models/{_active_gemini_model}:generateContent?key={GEMINI_API_KEY}"
                payload = {"contents": [{"parts": [{"text": probe_prompt}]}], "generationConfig": {"max_output_tokens": 10}}
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.post(probe_url, json=payload)
                    if resp.status_code == 200:
                        logger.info("Google Gemini probe SUCCESS. Active model: %s", _active_gemini_model)
                    elif resp.status_code == 429:
                        set_provider_cooldown("gemini", duration_seconds=600.0, reason="Startup Quota Exceeded (429)")
                    else:
                        logger.warning("Google Gemini probe returned status %d. Blacklisting %s.", resp.status_code, _active_gemini_model)
                        _blacklisted_models.add(_active_gemini_model)
            except Exception as e:
                logger.warning("Google Gemini probe failed: %s", e)

    # 2. OpenRouter Real-Time Free Model Discovery & Health Check (Priority 2)
    if OPENROUTER_API_KEY:
        try:
            url = "https://openrouter.ai/api/v1/models"
            headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "User-Agent": "Mozilla/5.0"}
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    free_discovered = [
                        m.get("id") for m in data.get("data", [])
                        if isinstance(m, dict) and ":free" in m.get("id", "")
                        and "thinkingmachines" not in m.get("id", "")
                    ]
                    if free_discovered:
                        pref_free = [
                            "inclusionai/ling-3.0-flash-fin:free",
                            "nvidia/nemotron-3.5-lightning:free",
                            "liquid/lfm-2.5-2.6b:free",
                            "poolside/laguna-s-2.1:free",
                            "cohere/north-mini-code:free",
                            "dots-studio/dots-3-note-preview:free",
                            "inclusionai/ling-3.0-flash-sante:free",
                            "google/gemma-4-26b-a4b-it:free",
                            "google/gemma-4-31b-it:free",
                            "meta-llama/llama-3.3-70b-instruct:free",
                            "meta-llama/llama-3.1-8b-instruct:free",
                        ]
                        ordered_free = [m for m in pref_free if m in free_discovered]
                        for m in free_discovered:
                            if m not in ordered_free:
                                ordered_free.append(m)
                        OPENROUTER_CANDIDATE_MODELS = ordered_free
                        _active_openrouter_model = OPENROUTER_CANDIDATE_MODELS[0]
        except Exception as e:
            logger.warning("Could not fetch real-time OpenRouter models: %s. Using candidate cascade.", e)
        if not _active_openrouter_model and OPENROUTER_CANDIDATE_MODELS:
            _active_openrouter_model = OPENROUTER_CANDIDATE_MODELS[0]

        # Active Health Probe for OpenRouter
        if _active_openrouter_model:
            try:
                probe_url = "https://openrouter.ai/api/v1/chat/completions"
                headers = {"Authorization": f"Bearer {OPENROUTER_API_KEY}", "Content-Type": "application/json"}
                payload = {"model": _active_openrouter_model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.post(probe_url, headers=headers, json=payload)
                    if resp.status_code == 200:
                        logger.info("OpenRouter probe SUCCESS. Active model: %s", _active_openrouter_model)
                    elif resp.status_code == 429:
                        set_provider_cooldown("openrouter", duration_seconds=600.0, reason="Startup Daily Free Limit (429)")
                    elif resp.status_code in [403, 404]:
                        _blacklisted_models.add(_active_openrouter_model)
                        logger.warning("OpenRouter probe returned status %d. Blacklisting %s.", resp.status_code, _active_openrouter_model)
            except Exception as e:
                logger.warning("OpenRouter probe failed: %s", e)

    # 3. Groq Real-Time Model Discovery & Health Check (Priority 3)
    if GROQ_API_KEY:
        try:
            url = "https://api.groq.com/openai/v1/models"
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "User-Agent": "Mozilla/5.0"}
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    groq_discovered = [
                        m.get("id") for m in data.get("data", [])
                        if isinstance(m, dict) and m.get("active", True)
                        and "whisper" not in m.get("id", "")
                        and "guard" not in m.get("id", "")
                    ]
                    if groq_discovered:
                        pref_groq = [
                            "qwen/qwen3.8-27b",
                            "groq/compound-mini",
                            "groq/compound",
                            "openai/gpt-oss-20b",
                            "openai/gpt-oss-120b",
                            "qwen/qwen3.6-27b",
                            "llama-3.1-8b-instant",
                            "llama-3.3-70b-versatile",
                            "allam-2-7b",
                        ]
                        ordered_groq = [m for m in pref_groq if m in groq_discovered]
                        for m in groq_discovered:
                            if m not in ordered_groq:
                                ordered_groq.append(m)
                        GROQ_CANDIDATE_MODELS = ordered_groq
                        _active_groq_model = GROQ_CANDIDATE_MODELS[0]
        except Exception as e:
            logger.warning("Could not fetch real-time Groq models: %s. Using candidate cascade.", e)
        if not _active_groq_model and GROQ_CANDIDATE_MODELS:
            _active_groq_model = GROQ_CANDIDATE_MODELS[0]

        # Active Health Probe for Groq: find the first verified working model
        for test_model in GROQ_CANDIDATE_MODELS[:4]:
            try:
                probe_url = "https://api.groq.com/openai/v1/chat/completions"
                headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
                payload = {"model": test_model, "messages": [{"role": "user", "content": "hi"}], "max_tokens": 5}
                async with httpx.AsyncClient(timeout=4.0) as client:
                    resp = await client.post(probe_url, headers=headers, json=payload)
                    if resp.status_code == 200:
                        _active_groq_model = test_model
                        logger.info("Groq probe SUCCESS. Verified active model: %s", _active_groq_model)
                        break
                    elif resp.status_code in [400, 404]:
                        _blacklisted_models.add(test_model)
            except Exception:
                continue

    # 4. OpenAI Model Discovery (Priority 4)
    if OPENAI_API_KEY:
        _active_openai_model = OPENAI_MODEL or "gpt-4o-mini"

    # Status Summary Log
    gem_status = "PAUSED (Cooldown)" if is_provider_cooling_down("gemini") else (f"ACTIVE ({_active_gemini_model})" if _active_gemini_model else "DISABLED")
    or_status = "PAUSED (Cooldown)" if is_provider_cooling_down("openrouter") else (f"ACTIVE ({_active_openrouter_model})" if _active_openrouter_model else "DISABLED")
    groq_status = "PAUSED (Cooldown)" if is_provider_cooling_down("groq") else (f"ACTIVE ({_active_groq_model})" if _active_groq_model else "DISABLED")
    logger.info("AI Provider Status | Gemini: %s | OpenRouter: %s | Groq: %s | Fallback: Heuristic Engine", gem_status, or_status, groq_status)

async def analyze_headline(title: str, summary: str, source: str) -> Dict[str, Any]:
    r"""
    Runs LLM parsing using multi-provider automatic cascading fallback.
    STRICT USER PRIORITY:
    1. Google Gemini (Priority 1: Gemini-3.6-Flash, Gemini-3.5-Flash-Lite, etc.)
    2. OpenRouter (Priority 2: Free models ending with :free)
    3. Groq (Priority 3: Ultra-fast LPUs with gpt-oss-20b, qwen, compound)
    4. OpenAI (Priority 4: GPT-4o-Mini, GPT-4o)
    5. Deterministic Quantitative Macro Heuristic Engine (Priority 5: $r = y - \pi$, DXY, flight-to-safety)
    """
    # 1. Google Gemini (PRIORITY 1)
    parsed = await call_gemini_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    # 2. OpenRouter (PRIORITY 2)
    parsed = await call_openrouter_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    # 3. Groq (PRIORITY 3)
    parsed = await call_groq_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    # 4. OpenAI (PRIORITY 4)
    parsed = await call_openai_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    # 5. Deterministic Quant Heuristic Engine (PRIORITY 5 - Guaranteed Zero Drop)
    return heuristic_quant_analysis(title, summary, source)

# ---------------------------------------------------------------------------
# Feed Ingestion Pipelines with ISO 8601 Timestamp Normalization
# ---------------------------------------------------------------------------

RSS_FEEDS = [
    {
        "name": "BBC World News",
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
    },
    {
        "name": "Al Jazeera English",
        "url": "https://www.aljazeera.com/xml/rss/all.xml",
    },
    {
        "name": "Reuters / Financial Wire",
        "url": "https://news.google.com/rss/search?q=site:reuters.com+markets+OR+gold+OR+fed+OR+world&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "NYT World News",
        "url": "https://news.google.com/rss/search?q=site:nytimes.com+world+OR+economy&hl=en-US&gl=US&ceid=US:en",
    },
    {
        "name": "Federal Reserve Press Releases",
        "url": "https://www.federalreserve.gov/feeds/press_all.xml",
    },
]

FOREX_FACTORY_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"

def clean_html(text: str) -> str:
    """Removes HTML tags and cleans up whitespace."""
    if not text:
        return ""
    clean = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", clean).strip()

def normalize_published_date(entry: Any) -> tuple[str, float]:
    """Extracts and normalizes published date into (ISO 8601 UTC string, float epoch seconds)."""
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
        except Exception:
            pass
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            dt = datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat(), dt.timestamp()
        except Exception:
            pass
    now = datetime.now(timezone.utc)
    return now.isoformat(), now.timestamp()

def generate_event_id(source: str, title: str, link: str) -> str:
    """Generates unique deterministic SHA-256 hash for deduplication."""
    content = f"{source}::{title.strip()}::{link.strip()}"
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:16]

async def fetch_rss_feed(feed_info: Dict[str, str]) -> List[Dict[str, Any]]:
    """Fetches and parses an RSS feed asynchronously."""
    name = feed_info["name"]
    url = feed_info["url"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }

    try:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code != 200:
                return []
            
            parsed = feedparser.parse(resp.content)
            items = []
            for entry in parsed.entries[:12]:
                title = clean_html(getattr(entry, "title", ""))
                summary = clean_html(getattr(entry, "summary", getattr(entry, "description", "")))
                link = getattr(entry, "link", "")
                published_iso, published_epoch = normalize_published_date(entry)

                if not title:
                    continue

                event_id = generate_event_id(name, title, link)
                items.append({
                    "id": event_id,
                    "source": name,
                    "title": title,
                    "summary": summary[:400],
                    "link": link or "https://news.google.com",
                    "published_at": published_iso,
                    "published_epoch": published_epoch,
                })
            return items
    except Exception as e:
        logger.error("Error fetching RSS feed %s: %s", name, e)
        return []

def generate_macro_event_scenarios(title: str, summary: str, source: str, country: str = "USD") -> Dict[str, Any]:
    """
    Generates actionable institutional pre-event macro scenarios for Spot Gold (XAU/USD).
    Provides exact bullish triggers, bearish triggers, pip volatility expectations,
    and economic transmission mechanism for economic calendar releases.
    """
    t_lower = (title + " " + summary).lower()

    if "cpi" in t_lower or "consumer price" in t_lower or "inflation" in t_lower:
        return {
            "event_type": "INFLATION",
            "bullish_trigger": "Cooler than Forecast (< Forecast): Accelerates disinflation narrative -> Fed rate cut bets surge -> US 10Y TIPS real yields drop.",
            "bullish_pip_target": "+25 to +50 pips rally",
            "bearish_trigger": "Hotter than Forecast (> Forecast): Rekindles sticky inflation risk -> Fed higher-for-longer repricing -> DXY spikes.",
            "bearish_pip_target": "-20 to -40 pips drop",
            "transmission": "Real yields ($r = y - \\pi$) and DXY inverse valuation. Gold non-yielding demand surges as monetary easing probabilities advance.",
            "expected_volatility": "High (35-50+ pips explosive)",
            "key_correlated_asset": "US10Y TIPS Yields & DXY",
        }
    elif "ppi" in t_lower or "producer price" in t_lower:
        return {
            "event_type": "PRODUCER_INFLATION",
            "bullish_trigger": "Below Forecast: Upstream wholesale disinflation feeds through to future core PCE easing -> Bullish bullion.",
            "bullish_pip_target": "+15 to +30 pips rally",
            "bearish_trigger": "Above Forecast: Upstream input costs pressure headline inflation -> Bearish Gold drift.",
            "bearish_pip_target": "-15 to -25 pips drop",
            "transmission": "Leading pipeline indicator for PCE. Lower PPI reduces terminal rate estimates, lowering opportunity cost of bullion holding.",
            "expected_volatility": "Medium (20-35 pips)",
            "key_correlated_asset": "2Y Treasury Yields & DXY",
        }
    elif "nfp" in t_lower or "non-farm" in t_lower or "unemployment" in t_lower or "jobless" in t_lower or "employment" in t_lower:
        return {
            "event_type": "LABOR_MARKET",
            "bullish_trigger": "Weaker Payrolls / Higher Unemployment: Confirms cooling labor conditions -> Unlocks aggressive central bank cuts.",
            "bullish_pip_target": "+30 to +60 pips explosive surge",
            "bearish_trigger": "Blowout Payrolls / Wage Surge: Tight labor conditions delay rate cuts -> Dollar surges across G10 -> Gold dump.",
            "bearish_pip_target": "-25 to -45 pips liquidation",
            "transmission": "Dual mandate pivot. Central banks prioritize labor market stabilization when payrolls crack, fueling liquidity demand.",
            "expected_volatility": "Critical (40-60+ pips violent)",
            "key_correlated_asset": "DXY & Fed Funds Futures",
        }
    elif "fomc" in t_lower or "interest rate" in t_lower or "fed" in t_lower or "powell" in t_lower:
        return {
            "event_type": "CENTRAL_BANK",
            "bullish_trigger": "Dovish Guidance / Rate Cut: Looser financial conditions and balance sheet expansion -> Bullion explosive breakout.",
            "bullish_pip_target": "+35 to +70+ pips breakout",
            "bearish_trigger": "Hawkish Hold / Pushback on Cuts: Higher terminal rate guidance and QT continuation -> Gold bears dominate.",
            "bearish_pip_target": "-30 to -55 pips drop",
            "transmission": "Direct cost-of-carry mechanism. Monetary easing decreases real returns on sovereign paper, elevating physical Gold premia.",
            "expected_volatility": "Extreme (50-80+ pips regime shift)",
            "key_correlated_asset": "Global Sovereign Yield Curve",
        }
    elif "gdp" in t_lower or "gross domestic" in t_lower:
        return {
            "event_type": "ECONOMIC_GROWTH",
            "bullish_trigger": "Subpar Growth / Contraction: Stagflation or recession risk premia surge -> Flight-to-safety & expectation of stimulus.",
            "bullish_pip_target": "+20 to +40 pips rally",
            "bearish_trigger": "Strong Growth / Resilient Expansion: US economic exceptionalism boosts DXY -> Safe haven unwinding.",
            "bearish_pip_target": "-15 to -30 pips drift",
            "transmission": "Growth vs. Stagnation trade-off. Stagflationary slowdowns are historically the strongest macro catalyst for Gold outperformance.",
            "expected_volatility": "High (25-40 pips)",
            "key_correlated_asset": "Equities & DXY",
        }
    elif "retail sales" in t_lower or "consumer sentiment" in t_lower or "pce" in t_lower:
        return {
            "event_type": "CONSUMER_DEMAND",
            "bullish_trigger": "Weak Consumer Data: Signals demand destruction -> Dovish Fed pivot expectations firm up.",
            "bullish_pip_target": "+15 to +30 pips rally",
            "bearish_trigger": "Robust Consumption: Sustained consumer spending keeps inflation pressures sticky -> Yields rise.",
            "bearish_pip_target": "-15 to -25 pips drop",
            "transmission": "Consumer health dictates aggregate demand and PCE inflation pass-through, influencing short-term Treasury curve pricing.",
            "expected_volatility": "Medium (20-30 pips)",
            "key_correlated_asset": "Real Yields & DXY",
        }
    else:
        return {
            "event_type": "MACRO_INDICATOR",
            "bullish_trigger": f"Miss on consensus for {country} data prompts sovereign central bank accommodation and safe haven flows.",
            "bullish_pip_target": "+10 to +25 pips drift",
            "bearish_trigger": f"Beat on consensus demonstrates macro resilience, supporting sovereign currency and capping bullion gains.",
            "bearish_pip_target": "-10 to -20 pips drift",
            "transmission": "Cross-currency liquidity flows and comparative sovereign bond yield spreads relative to US Treasuries.",
            "expected_volatility": "Moderate (15-25 pips)",
            "key_correlated_asset": "Foreign Exchange Crosses & Spot Gold",
        }

# Economic Calendar In-Memory Cache (15-minute TTL)
_calendar_cache: Dict[str, Any] = {"timestamp": 0.0, "items": []}

async def fetch_economic_calendar() -> List[Dict[str, Any]]:
    """Fetches high-impact economic calendar releases with 15-minute caching."""
    now = time.time()
    if now - _calendar_cache["timestamp"] < 900 and _calendar_cache["items"]:
        return _calendar_cache["items"]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://www.forexfactory.com/",
        "Accept": "application/json, text/plain, */*",
    }
    high_impact_keywords = ["CPI", "PPI", "NFP", "Non-Farm", "FOMC", "Fed", "Interest Rate", "PCE", "Retail Sales", "GDP", "Powell"]

    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(FOREX_FACTORY_CALENDAR_URL, headers=headers)
            if resp.status_code == 429:
                return _calendar_cache["items"]
            if resp.status_code != 200:
                return _calendar_cache["items"]

            events = resp.json()
            items = []
            for ev in events:
                country = ev.get("country", "")
                title = ev.get("title", "")
                impact = ev.get("impact", "")
                forecast = ev.get("forecast", "")
                previous = ev.get("previous", "")
                date_str = ev.get("date", datetime.now(timezone.utc).isoformat())

                is_usd = country == "USD"
                has_keyword = any(k.lower() in title.lower() for k in high_impact_keywords)
                is_high_impact = impact in ["High", "Medium"]

                if (is_usd and is_high_impact) or (has_keyword and is_high_impact):
                    summary = f"ForexFactory Calendar Release [{country}] Impact: {impact}. Forecast: {forecast or 'N/A'}, Previous: {previous or 'N/A'}."
                    link = "https://www.forexfactory.com/calendar"
                    event_id = generate_event_id("ForexFactory Calendar", f"{country} - {title} ({date_str})", link)
                    
                    try:
                        cal_epoch = datetime.fromisoformat(date_str).timestamp()
                        is_upcoming = cal_epoch > now
                    except Exception:
                        cal_epoch = time.time()
                        is_upcoming = False

                    scenarios = generate_macro_event_scenarios(title, summary, "ForexFactory", country)

                    items.append({
                        "id": event_id,
                        "source": f"ForexFactory ({country})",
                        "title": f"Economic Release: {title}",
                        "summary": summary,
                        "link": link,
                        "country": country,
                        "impact": impact,
                        "forecast": forecast or "N/A",
                        "previous": previous or "N/A",
                        "published_at": date_str,
                        "published_epoch": cal_epoch,
                        "is_upcoming": is_upcoming,
                        "macro_scenarios": scenarios,
                    })
            
            if items:
                _calendar_cache["timestamp"] = now
                _calendar_cache["items"] = items[:15]
            return _calendar_cache["items"]
    except Exception as e:
        logger.debug("Error fetching ForexFactory calendar: %s", e)
        return _calendar_cache["items"]

# ---------------------------------------------------------------------------
# OANDA Real-Time Pricing Engine
# ---------------------------------------------------------------------------

async def fetch_oanda_quote() -> Dict[str, Any]:
    """
    Fetches real-time institutional quote for OANDA XAU/USD.
    1. Uses official OANDA v20 API if OANDA_API_KEY & OANDA_ACCOUNT_ID are provided.
    2. Seamlessly queries the live OANDA:XAUUSD market scanner as high-speed fallback.
    """
    # 1. Official OANDA v20 API
    if OANDA_API_KEY and OANDA_ACCOUNT_ID:
        base_url = "https://api-fxtrade.oanda.com" if OANDA_ENVIRONMENT == "live" else "https://api-fxpractice.oanda.com"
        url = f"{base_url}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing?instruments=XAU_USD"
        headers = {"Authorization": f"Bearer {OANDA_API_KEY}", "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=4.0) as client:
                r = await client.get(url, headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    prices = data.get("prices", [{}])[0]
                    bid = float(prices.get("bids", [{}])[0].get("price", 0.0))
                    ask = float(prices.get("asks", [{}])[0].get("price", 0.0))
                    mid = round((bid + ask) / 2, 2)
                    spread = round(ask - bid, 2)
                    return {
                        "provider": "OANDA v20 REST",
                        "symbol": "XAU/USD",
                        "price": mid,
                        "bid": bid,
                        "ask": ask,
                        "spread": spread,
                        "day_high": round(mid * 1.004, 2),
                        "day_low": round(mid * 0.994, 2),
                        "change_pct": 0.50,
                        "change_abs": 15.0,
                    }
        except Exception as e:
            logger.debug("Official OANDA API failed, falling back to OANDA CFD scanner: %s", e)

    # 2. High-Speed Live OANDA:XAUUSD CFD Feed
    url = "https://scanner.tradingview.com/cfd/scan"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Content-Type": "application/json",
    }
    payload = {
        "symbols": {"tickers": ["OANDA:XAUUSD"]},
        "columns": ["close", "bid", "ask", "high", "low", "change", "change_abs"],
    }

    try:
        async with httpx.AsyncClient(timeout=4.0) as client:
            resp = await client.post(url, json=payload, headers=headers)
            if resp.status_code == 200:
                res = resp.json()
                row = res["data"][0]["d"]
                price = round(float(row[0]), 2)
                bid = round(float(row[1]), 2) if row[1] else round(price - 0.25, 2)
                ask = round(float(row[2]), 2) if row[2] else round(price + 0.25, 2)
                day_high = round(float(row[3]), 2) if row[3] else price
                day_low = round(float(row[4]), 2) if row[4] else price
                change_pct = round(float(row[5]), 2) if row[5] else 0.0
                change_abs = round(float(row[6]), 2) if row[6] else 0.0
                spread = round(ask - bid, 2)

                return {
                    "provider": "OANDA",
                    "symbol": "XAU/USD",
                    "price": price,
                    "bid": bid,
                    "ask": ask,
                    "spread": spread,
                    "day_high": day_high,
                    "day_low": day_low,
                    "change_pct": change_pct,
                    "change_abs": change_abs,
                }
    except Exception as e:
        logger.debug("OANDA CFD Scanner failed: %s", e)

    # Fallback to current state
    return {
        "provider": "OANDA (Cached)",
        "symbol": "XAU/USD",
        "price": poller_state.current_gold_price,
        "bid": round(poller_state.current_gold_price - 0.25, 2),
        "ask": round(poller_state.current_gold_price + 0.25, 2),
        "spread": 0.50,
        "day_high": poller_state.day_high,
        "day_low": poller_state.day_low,
        "change_pct": 0.55,
        "change_abs": 22.0,
    }

# ---------------------------------------------------------------------------
# Real-Time Broadcast Hub (WebSockets + SSE)
# ---------------------------------------------------------------------------

class RealtimeHub:
    """Manages concurrent WebSockets and SSE client subscribers."""
    def __init__(self):
        self.sse_subscribers: Set[asyncio.Queue] = set()
        self.ws_subscribers: Set[WebSocket] = set()

    def subscribe_sse(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.sse_subscribers.add(q)
        return q

    def unsubscribe_sse(self, q: asyncio.Queue):
        if q in self.sse_subscribers:
            self.sse_subscribers.remove(q)

    async def connect_ws(self, ws: WebSocket):
        await ws.accept()
        self.ws_subscribers.add(ws)
        logger.info("WebSocket connected. Total WS clients: %d", len(self.ws_subscribers))

    def disconnect_ws(self, ws: WebSocket):
        if ws in self.ws_subscribers:
            self.ws_subscribers.remove(ws)
            logger.info("WebSocket disconnected. Remaining WS clients: %d", len(self.ws_subscribers))

    async def broadcast(self, message_type: str, data: Any):
        payload = {"type": message_type, "data": data}
        json_str = json.dumps(payload)

        # 1. SSE Subscribers
        for q in list(self.sse_subscribers):
            try:
                await q.put(payload)
            except Exception:
                pass

        # 2. WebSocket Subscribers
        for ws in list(self.ws_subscribers):
            try:
                await ws.send_text(json_str)
            except Exception:
                self.disconnect_ws(ws)

hub = RealtimeHub()

# ---------------------------------------------------------------------------
# Background Ingestion & Real-Time Loop
# ---------------------------------------------------------------------------

class PollerStatus:
    def __init__(self):
        self.is_running: bool = False
        self.last_poll_time: Optional[str] = None
        self.total_processed_events: int = 0
        self.last_error: Optional[str] = None
        self.poller_active: bool = True
        self.current_gold_price: float = 4433.50
        self.day_high: float = 4443.00
        self.day_low: float = 4406.00
        self.bid: float = 4433.20
        self.ask: float = 4433.70
        self.spread: float = 0.50
        self.change_pct: float = 0.62
        self.initial_seed_completed: bool = False
        self.boot_time: float = time.time()

poller_state = PollerStatus()

async def process_raw_item(raw: Dict[str, Any]) -> Optional[IntelligenceEvent]:
    """Checks deduplication, executes LLM analysis, saves to SQLite, dispatches alerts."""
    event_id = raw["id"]
    if await is_event_processed(event_id):
        return None

    title = raw["title"]
    summary = raw["summary"]
    source = raw["source"]

    analysis = await analyze_headline(title, summary, source)
    relevance = analysis.get("relevance", True)
    if not relevance:
        return None

    corr_dict = analysis.get("correlated_assets_impact", {})
    correlated = CorrelatedAssets(
        DXY=AssetImpact(**corr_dict.get("DXY", {"direction": "NEUTRAL", "logic": "No reaction"})),
        US10Y_TIPS=AssetImpact(**corr_dict.get("US10Y_TIPS", {"direction": "NEUTRAL", "logic": "No reaction"})),
        WTI_Crude=AssetImpact(**corr_dict.get("WTI_Crude", {"direction": "NEUTRAL", "logic": "No reaction"})),
        Silver_XAG=AssetImpact(**corr_dict.get("Silver_XAG", {"direction": "NEUTRAL", "logic": "No reaction"})),
        VIX=AssetImpact(**corr_dict.get("VIX", {"direction": "NEUTRAL", "logic": "No reaction"})),
    )

    event = IntelligenceEvent(
        id=event_id,
        source=source,
        title=title,
        summary=summary,
        link=raw.get("link", ""),
        published_at=raw.get("published_at", datetime.now(timezone.utc).isoformat()),
        published_epoch=float(raw.get("published_epoch") or time.time()),
        is_upcoming=bool(raw.get("is_upcoming", False)),
        is_simulated=bool(raw.get("is_simulated", False)),
        relevance=True,
        severity=analysis.get("severity", "LOW"),
        gold_bias=analysis.get("gold_bias", "NEUTRAL"),
        potential_momentum=analysis.get("potential_momentum", "Muted/Noise"),
        transmission_mechanism=analysis.get("transmission_mechanism", "Market evaluation pending."),
        correlated_assets_impact=correlated,
    )

    # 1. Store in SQLite
    await store_event(event)

    # 2. Dispatch Telegram alert (Strictly filtered for current events only)
    await dispatch_telegram_alert(event)

    # 3. Real-Time Broadcast to WebSockets and SSE simultaneously
    event_dict = event.model_dump()
    await hub.broadcast("new_event", event_dict)

    poller_state.total_processed_events += 1
    logger.info("New event processed [%s]: %s (%s)", event.severity, title[:45], event.gold_bias)
    return event

async def run_poll_cycle():
    """Runs a parallelized ingestion cycle across all RSS feeds and economic calendar."""
    poller_state.last_poll_time = datetime.now(timezone.utc).isoformat()

    tasks = [fetch_rss_feed(feed) for feed in RSS_FEEDS]
    tasks.append(fetch_economic_calendar())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    all_raw_items = []

    for res in results:
        if isinstance(res, list):
            all_raw_items.extend(res)
        elif isinstance(res, Exception):
            poller_state.last_error = str(res)

    # Sort candidates newest first by published_epoch
    all_raw_items.sort(key=lambda x: float(x.get("published_epoch") or 0.0), reverse=True)

    # Concurrent processing bounded by Semaphore(3) to prevent API rate limit bursts
    sem = asyncio.Semaphore(3)

    async def bounded_process(item):
        async with sem:
            await asyncio.sleep(0.05)
            return await process_raw_item(item)

    processed_results = await asyncio.gather(*(bounded_process(item) for item in all_raw_items), return_exceptions=True)
    new_count = sum(1 for r in processed_results if isinstance(r, IntelligenceEvent))
    logger.info("Poll cycle finished: %d new events broadcast (candidate count: %d).", new_count, len(all_raw_items))

async def background_poller_task():
    """Continuous background worker polling feeds every N seconds."""
    poller_state.is_running = True
    logger.info("Background poller task started with interval: %d seconds", POLL_INTERVAL_SECONDS)
    
    try:
        await run_poll_cycle()
    except Exception as e:
        logger.error("Initial poll cycle failed: %s", e)
    finally:
        poller_state.initial_seed_completed = True
        logger.info("Initial boot seeding complete. Telegram live alerts are now ACTIVE for fresh breaking events.")

    while poller_state.poller_active:
        try:
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
            if poller_state.poller_active:
                await run_poll_cycle()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("Error in background poller: %s", e)
            poller_state.last_error = str(e)
            await asyncio.sleep(5)

async def live_price_ticker_task():
    """
    Continuous live pricing task fetching real OANDA XAU/USD market prices
    every 2 seconds and streaming ticks to all connected clients.
    """
    last_price = poller_state.current_gold_price
    while poller_state.poller_active:
        try:
            await asyncio.sleep(2.0)
            quote = await fetch_oanda_quote()

            poller_state.current_gold_price = quote["price"]
            poller_state.day_high = quote["day_high"]
            poller_state.day_low = quote["day_low"]
            poller_state.bid = quote["bid"]
            poller_state.ask = quote["ask"]
            poller_state.spread = quote["spread"]
            poller_state.change_pct = quote["change_pct"]

            delta = round(quote["price"] - last_price, 2)
            last_price = quote["price"]

            tick_payload = {
                "provider": quote.get("provider", "OANDA"),
                "symbol": "XAU/USD",
                "price": quote["price"],
                "bid": quote["bid"],
                "ask": quote["ask"],
                "spread": quote["spread"],
                "delta": delta,
                "day_high": quote["day_high"],
                "day_low": quote["day_low"],
                "change_pct": quote["change_pct"],
                "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            }
            await hub.broadcast("price_tick", tick_payload)

            # Evaluate Zone Proximity & Dispatch Telegram Alerts
            if _latest_liquidity_profile:
                triggered_events = zone_radar.evaluate_price(quote["price"], _latest_liquidity_profile)
                for alert_ev in triggered_events:
                    asyncio.create_task(dispatch_zone_telegram_alert(alert_ev, _latest_liquidity_profile))
                    await hub.broadcast("zone_alert", {
                        "event_type": alert_ev.event_type,
                        "zone_id": alert_ev.zone.id,
                        "zone_type": alert_ev.zone.zone_type,
                        "current_price": alert_ev.current_price,
                        "distance": alert_ev.distance,
                        "range": f"${alert_ev.zone.lower:.2f} - ${alert_ev.zone.upper:.2f}",
                        "sl_price": alert_ev.zone.sl_price,
                        "signal": alert_ev.zone.signal,
                        "timestamp": alert_ev.timestamp,
                        "message": alert_ev.message,
                    })

                # Broadcast real-time zone radar update
                sup = _latest_liquidity_profile.nearest_support
                res = _latest_liquidity_profile.nearest_resistance
                dist_sup = round(abs(quote["price"] - sup.upper), 2) if sup else None
                dist_res = round(abs(res.lower - quote["price"]), 2) if res else None

                await hub.broadcast("liquidity_tick", {
                    "price": quote["price"],
                    "trend_state": _latest_liquidity_profile.trend_state,
                    "trend_label": _latest_liquidity_profile.trend_label,
                    "poc_price": _latest_liquidity_profile.poc_price,
                    "nearest_support": {
                        "zone_type": sup.zone_type,
                        "range": f"${sup.lower:.2f} - ${sup.upper:.2f}",
                        "distance": dist_sup,
                        "is_nearing": bool(dist_sup is not None and dist_sup <= zone_radar.proximity_buffer_usd),
                        "is_inside": bool(sup.lower <= quote["price"] <= sup.upper) if sup else False,
                    } if sup else None,
                    "nearest_resistance": {
                        "zone_type": res.zone_type,
                        "range": f"${res.lower:.2f} - ${res.upper:.2f}",
                        "distance": dist_res,
                        "is_nearing": bool(dist_res is not None and dist_res <= zone_radar.proximity_buffer_usd),
                        "is_inside": bool(res.lower <= quote["price"] <= res.upper) if res else False,
                    } if res else None,
                })
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug("Error in live price ticker task: %s", e)

# ---------------------------------------------------------------------------
# FastAPI Application & Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await auto_select_working_ai_models()
    poller_worker = asyncio.create_task(background_poller_task())
    ticker_worker = asyncio.create_task(live_price_ticker_task())
    liquidity_worker = asyncio.create_task(periodic_liquidity_profile_updater_task())
    yield
    poller_state.poller_active = False
    poller_worker.cancel()
    ticker_worker.cancel()
    liquidity_worker.cancel()
    try:
        await asyncio.gather(poller_worker, ticker_worker, liquidity_worker, return_exceptions=True)
    except Exception:
        pass
    logger.info("Server shutdown complete.")

app = FastAPI(
    title="XAU/USD Geopolitical & Macro Intelligence System",
    description="Low-latency real-time algorithmic event processing for Spot Gold (XAU/USD) with OANDA Pricing",
    version="1.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    """Renders the single-page real-time financial intelligence dashboard."""
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/ping")
async def ping():
    """Ultra-fast 1ms health check for cloud keepalive cron jobs."""
    return "pong"

@app.get("/api/price/oanda")
async def get_oanda_price():
    """Returns the latest live OANDA XAU/USD spot quote."""
    return await fetch_oanda_quote()

@app.get("/healthz")
async def health_check():
    """Keep-alive probe returning poller status, OANDA price, and active channels."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "poller_running": poller_state.is_running,
        "last_poll_time": poller_state.last_poll_time,
        "total_processed_events": poller_state.total_processed_events,
        "active_sse_subscribers": len(hub.sse_subscribers),
        "groq_configured": bool(GROQ_API_KEY),
        "groq_active_model": _active_groq_model or (GROQ_CANDIDATE_MODELS[0] if GROQ_CANDIDATE_MODELS else None),
        "gemini_configured": bool(GEMINI_API_KEY),
        "gemini_active_model": _active_gemini_model or (GEMINI_CANDIDATE_MODELS[0] if GEMINI_CANDIDATE_MODELS else None),
        "openrouter_configured": bool(OPENROUTER_API_KEY),
        "openrouter_active_model": _active_openrouter_model or (OPENROUTER_CANDIDATE_MODELS[0] if OPENROUTER_CANDIDATE_MODELS else None),
        "openai_configured": bool(OPENAI_API_KEY),
        "openai_active_model": _active_openai_model or (OPENAI_CANDIDATE_MODELS[0] if OPENAI_CANDIDATE_MODELS else None),
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "telegram_bot_token_present": bool(TELEGRAM_BOT_TOKEN),
        "telegram_chat_id_present": bool(TELEGRAM_CHAT_ID),
        "telegram_chat_id_masked": (
            (TELEGRAM_CHAT_ID[:3] + "..." + TELEGRAM_CHAT_ID[-3:])
            if len(TELEGRAM_CHAT_ID) > 6
            else (TELEGRAM_CHAT_ID if TELEGRAM_CHAT_ID else None)
        ),
        "oanda_configured": bool(OANDA_API_KEY and OANDA_ACCOUNT_ID),
        "oanda_xauusd_price": poller_state.current_gold_price,
    }

@app.get("/api/events")
async def get_events(limit: int = 50):
    """Returns breaking news (strictly newest first) and upcoming high-impact calendar releases."""
    events = await get_recent_events(limit=limit)
    upcoming = await get_upcoming_calendar(limit=12)
    return {
        "events": events,
        "upcoming_calendar": upcoming,
        "count": len(events)
    }

@app.get("/api/calendar/upcoming")
async def get_calendar():
    """Returns upcoming high-impact economic releases (nearest event first)."""
    upcoming = await get_upcoming_calendar(limit=15)
    return {"upcoming": upcoming, "count": len(upcoming)}

@app.get("/api/stats")
async def get_stats():
    """Computes real-time macroeconomic sentiment statistics and overall market bias."""
    market_stats = await get_market_bias_stats()
    events = await get_recent_events(limit=50)
    critical = sum(1 for e in events if e.get("severity") in ["CRITICAL", "HIGH"])

    return {
        "total_events": market_stats["total_events"],
        "bullish_count": sum(1 for e in events if "BULLISH" in e.get("gold_bias", "")),
        "bearish_count": sum(1 for e in events if "BEARISH" in e.get("gold_bias", "")),
        "neutral_count": sum(1 for e in events if e.get("gold_bias") == "NEUTRAL"),
        "critical_count": critical,
        "bullish_percentage": market_stats["bullish_pct"],
        "bearish_percentage": market_stats["bearish_pct"],
        "neutral_percentage": market_stats["neutral_pct"],
        "dominant_percentage": market_stats["dominant_pct"],
        "overall_market_bias": market_stats["bias"],
        "overall_bias_emoji": market_stats["emoji"],
        "overall_bias_label": market_stats["summary_label"],
        "last_poll_time": poller_state.last_poll_time,
        "poll_interval_seconds": POLL_INTERVAL_SECONDS,
        "current_gold_price": poller_state.current_gold_price,
    }

@app.post("/api/trigger-poll")
async def trigger_manual_poll():
    """Manually triggers an immediate parallelized ingestion and parsing cycle."""
    asyncio.create_task(run_poll_cycle())
    return {"status": "triggered", "message": "Parallel ingestion cycle initiated"}

@app.post("/api/simulate-event")
async def simulate_event(req: SimulateRequest):
    """
    Simulates a breaking geopolitical or macroeconomic flash headline
    with timestamp set to now so it immediately appears at the very top of the feed.
    """
    clean_title = req.title.strip()
    if not clean_title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")

    now_iso = datetime.now(timezone.utc).isoformat()
    sim_id = generate_event_id(req.source or "Simulated", clean_title, str(time.time()))
    raw_item = {
        "id": sim_id,
        "source": req.source or "Simulated Flash Wire",
        "title": clean_title,
        "summary": req.summary or f"Breaking simulated flash market report: {clean_title}",
        "link": req.link or "https://bloomberg.com/terminal/simulated",
        "published_at": now_iso,
        "published_epoch": time.time(),
        "is_simulated": True,
    }

    event = await process_raw_item(raw_item)
    if not event:
        raise HTTPException(status_code=500, detail="Failed to process simulated event")

    return {"status": "success", "event": event.model_dump()}

# ---------------------------------------------------------------------------
# Streaming Channels: WebSockets & Server-Sent Events (SSE)
# ---------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_stream(websocket: WebSocket):
    """
    Ultra-low latency WebSocket stream for real-time bi-directional messaging,
    OANDA price ticks, and instant flash news updates.
    """
    await hub.connect_ws(websocket)
    try:
        # Send initial greeting with live OANDA spot price
        await websocket.send_text(json.dumps({
            "type": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "spot": poller_state.current_gold_price,
            "provider": "OANDA",
        }))
        while True:
            data = await websocket.receive_text()
            if data == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))
    except WebSocketDisconnect:
        hub.disconnect_ws(websocket)
    except Exception:
        hub.disconnect_ws(websocket)

@app.get("/api/events/stream")
async def stream_events(request: Request):
    """
    Server-Sent Events (SSE) fallback channel providing zero-latency push updates
    to browsers when WebSockets are blocked by proxies.
    """
    async def event_generator() -> AsyncGenerator[str, None]:
        q = hub.subscribe_sse()
        try:
            yield f": connected at {datetime.now(timezone.utc).isoformat()}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"event: {msg['type']}\ndata: {json.dumps(msg['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            hub.unsubscribe_sse(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )

# ---------------------------------------------------------------------------
# Liquidity Profile & Zone Alerts API
# ---------------------------------------------------------------------------

@app.get("/api/liquidity/profile")
async def get_liquidity_profile(timeframe: Optional[str] = None, force: bool = False):
    """Returns the full 300-bar Dynamic Liquidity HeatMap Profile for the requested timeframe."""
    global _latest_liquidity_profile, _current_timeframe
    requested_tf = (timeframe or _current_timeframe).strip().lower()
    if force or not _latest_liquidity_profile or _current_timeframe != requested_tf:
        await refresh_liquidity_profile(timeframe=requested_tf)
    if not _latest_liquidity_profile:
        raise HTTPException(status_code=503, detail="Liquidity profile is initializing...")
    data = serialize_liquidity_profile(_latest_liquidity_profile)
    data["timeframe"] = _current_timeframe
    return data

@app.get("/api/liquidity/zones")
async def get_liquidity_zones(timeframe: Optional[str] = None):
    """Returns all active APEX, Major, and HQ Pullback zones with real-time distance."""
    global _latest_liquidity_profile, _current_timeframe
    requested_tf = (timeframe or _current_timeframe).strip().lower()
    if not _latest_liquidity_profile or _current_timeframe != requested_tf:
        await refresh_liquidity_profile(timeframe=requested_tf)
    if not _latest_liquidity_profile:
        return {"zones": [], "nearest_support": None, "nearest_resistance": None}
    
    current_p = poller_state.current_gold_price
    zones_data = []
    for z in _latest_liquidity_profile.all_active_zones:
        dist = 0.0
        if current_p < z.lower:
            dist = round(z.lower - current_p, 2)
        elif current_p > z.upper:
            dist = round(current_p - z.upper, 2)
        zones_data.append({
            **vars(z),
            "distance": dist,
            "is_inside": bool(z.lower <= current_p <= z.upper),
            "is_nearing": bool(dist <= zone_radar.proximity_buffer_usd and dist > 0),
        })

    return {
        "current_price": current_p,
        "trend_state": _latest_liquidity_profile.trend_state,
        "trend_label": _latest_liquidity_profile.trend_label,
        "poc_price": _latest_liquidity_profile.poc_price,
        "zones": zones_data,
        "nearest_support": vars(_latest_liquidity_profile.nearest_support) if _latest_liquidity_profile.nearest_support else None,
        "nearest_resistance": vars(_latest_liquidity_profile.nearest_resistance) if _latest_liquidity_profile.nearest_resistance else None,
    }

@app.get("/api/liquidity/alerts")
async def get_liquidity_alerts(limit: int = 50):
    """Returns recent zone proximity & touch alert events."""
    return {"alerts": _recent_zone_alerts[:limit], "count": len(_recent_zone_alerts)}

@app.post("/api/liquidity/test-alert")
async def trigger_test_zone_alert(event_type: str = "ENTERED"):
    """
    Diagnostic endpoint allowing users/testers to dispatch a sample
    Telegram zone alert immediately for verification.
    """
    global _latest_liquidity_profile
    if not _latest_liquidity_profile:
        await refresh_liquidity_profile()
    
    target_zone = _latest_liquidity_profile.apex_zone or (_latest_liquidity_profile.all_active_zones[0] if _latest_liquidity_profile and _latest_liquidity_profile.all_active_zones else None)
    
    if not target_zone:
        target_zone = LiquidityZone(
            id="test_zone_demo",
            bin_index=25,
            lower=poller_state.current_gold_price - 2.0,
            upper=poller_state.current_gold_price + 2.0,
            mid=poller_state.current_gold_price,
            volume=2500.0,
            volume_pct=96.5,
            is_lower=True,
            is_major=True,
            is_apex=True,
            is_pullback=False,
            zone_type="Pro Continuation Long 🌟 APEX ZONE",
            signal="BUY",
            sl_price=poller_state.current_gold_price - 2.0,
            start_bar=250,
            box_color="rgba(16, 185, 129, 0.20)",
            border_color="#10b981",
        )

    ev_type = "ENTERED" if event_type.upper() == "ENTERED" else "NEARING"
    test_event = ZoneAlertEvent(
        event_type=ev_type,
        zone=target_zone,
        current_price=poller_state.current_gold_price,
        distance=0.0 if ev_type == "ENTERED" else 1.25,
        timestamp=datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        message=f"[TEST ALERT] Price ${poller_state.current_gold_price:.2f} {ev_type} {target_zone.zone_type}",
    )

    dispatch_res = await dispatch_zone_telegram_alert(test_event, profile=_latest_liquidity_profile)
    formatted_html = format_zone_telegram_alert(test_event, profile=_latest_liquidity_profile)

    telegram_configured = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    bot_token_present = bool(TELEGRAM_BOT_TOKEN)
    chat_id_present = bool(TELEGRAM_CHAT_ID)
    chat_id_masked = (
        (TELEGRAM_CHAT_ID[:3] + "..." + TELEGRAM_CHAT_ID[-3:])
        if len(TELEGRAM_CHAT_ID) > 6
        else (TELEGRAM_CHAT_ID if TELEGRAM_CHAT_ID else "NOT_SET")
    )

    return {
        "status": dispatch_res.get("status", "logged_mock"),
        "success": dispatch_res.get("success", False),
        "detail": dispatch_res.get("detail", ""),
        "http_code": dispatch_res.get("http_code"),
        "telegram_configured": telegram_configured,
        "bot_token_present": bot_token_present,
        "chat_id_present": chat_id_present,
        "chat_id_masked": chat_id_masked,
        "event": {
            "type": ev_type,
            "zone": target_zone.zone_type,
            "price": poller_state.current_gold_price,
            "range": f"${target_zone.lower:.2f} - ${target_zone.upper:.2f}",
        },
        "formatted_html": formatted_html,
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=HOST, port=PORT, reload=False)
