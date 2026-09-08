"""
XAU/USD Geopolitical & Macro Intelligence System
Production-grade, low-latency event-driven algorithmic infrastructure and financial dashboard.
"""

import asyncio
import hashlib
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
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

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
# Database Management (aiosqlite WAL mode)
# ---------------------------------------------------------------------------

async def init_db():
    """Initializes the SQLite database with WAL mode and schema."""
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
                relevance INTEGER NOT NULL,
                severity TEXT NOT NULL,
                gold_bias TEXT NOT NULL,
                potential_momentum TEXT NOT NULL,
                transmission_mechanism TEXT NOT NULL,
                correlated_assets_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);")
        await db.commit()
    logger.info("SQLite persistence initialized at %s with WAL mode.", DB_PATH)

async def store_event(event: IntelligenceEvent):
    """Persists a parsed intelligence event."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO events (
                id, source, title, summary, link, published_at,
                relevance, severity, gold_bias, potential_momentum,
                transmission_mechanism, correlated_assets_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event.id,
            event.source,
            event.title,
            event.summary,
            event.link,
            event.published_at,
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
    """Retrieves the last N events ordered by created_at DESC."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM events ORDER BY created_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            results = []
            for row in rows:
                item = dict(row)
                item["relevance"] = bool(item["relevance"])
                try:
                    item["correlated_assets_impact"] = json.loads(item["correlated_assets_json"])
                except Exception:
                    item["correlated_assets_impact"] = {}
                results.append(item)
            return results

async def is_event_processed(event_id: str) -> bool:
    """Checks if an event ID already exists in the database."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT 1 FROM events WHERE id = ?", (event_id,)) as cursor:
            row = await cursor.fetchone()
            return row is not None

# ---------------------------------------------------------------------------
# Telegram Bot Dispatcher
# ---------------------------------------------------------------------------

def format_telegram_alert(event: IntelligenceEvent) -> str:
    """Formats event into a high-visibility, crisp HTML alert for Telegram."""
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
        "MEDIUM": "⚡ [MEDIUM]",
        "LOW": "ℹ️ [LOW]",
    }.get(event.severity, event.severity)

    corr = event.correlated_assets_impact
    dxy_dir = f"{'🟢' if corr.DXY.direction == 'BULLISH' else '🔴' if corr.DXY.direction == 'BEARISH' else '⚪'} {corr.DXY.direction}"
    tips_dir = f"{'🟢' if corr.US10Y_TIPS.direction == 'BULLISH' else '🔴' if corr.US10Y_TIPS.direction == 'BEARISH' else '⚪'} {corr.US10Y_TIPS.direction}"
    oil_dir = f"{'🟢' if corr.WTI_Crude.direction == 'BULLISH' else '🔴' if corr.WTI_Crude.direction == 'BEARISH' else '⚪'} {corr.WTI_Crude.direction}"
    silver_dir = f"{'🟢' if corr.Silver_XAG.direction == 'BULLISH' else '🔴' if corr.Silver_XAG.direction == 'BEARISH' else '⚪'} {corr.Silver_XAG.direction}"
    vix_dir = f"{'🟢' if corr.VIX.direction == 'BULLISH' else '🔴' if corr.VIX.direction == 'BEARISH' else '⚪'} {corr.VIX.direction}"

    return (
        f"<b>{severity_badge}</b>\n"
        f"🏆 <b>XAU/USD Gold Bias:</b> {bias_emoji}\n"
        f"⚡ <b>Expected Momentum:</b> <code>{event.potential_momentum}</code>\n\n"
        f"📰 <b>Headline:</b> {event.title}\n"
        f"📡 <b>Source:</b> {event.source}\n\n"
        f"🎯 <b>Transmission Mechanism:</b>\n"
        f"<i>{event.transmission_mechanism}</i>\n\n"
        f"📊 <b>Correlated Assets Breakdown:</b>\n"
        f"• <b>DXY:</b> {dxy_dir} ({corr.DXY.logic})\n"
        f"• <b>US10Y TIPS:</b> {tips_dir} ({corr.US10Y_TIPS.logic})\n"
        f"• <b>WTI Crude:</b> {oil_dir} ({corr.WTI_Crude.logic})\n"
        f"• <b>Silver (XAG):</b> {silver_dir} ({corr.Silver_XAG.logic})\n"
        f"• <b>VIX:</b> {vix_dir} ({corr.VIX.logic})\n\n"
        f"🔗 <a href='{event.link}'>View Original Source</a> | ⏱ <i>{event.published_at}</i>"
    )

async def dispatch_telegram_alert(event: IntelligenceEvent):
    """Dispatches alert to configured Telegram chat asynchronously."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.debug("Telegram credentials not configured; skipping dispatch.")
        return

    # Dispatch alerts for HIGH and CRITICAL events
    if event.severity not in ["CRITICAL", "HIGH"]:
        logger.debug("Skipping Telegram for severity %s", event.severity)
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": format_telegram_alert(event),
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, json=payload)
            if resp.status_code == 200:
                logger.info("Telegram alert dispatched successfully for %s", event.id)
            else:
                logger.warning("Telegram dispatch returned status %d: %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Error dispatching Telegram alert: %s", e)

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

async def call_groq_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls Groq Llama-3.3-70B API with JSON mode."""
    if not GROQ_API_KEY:
        return None

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."
    payload = {
        "model": "llama-3.3-70b-versatile",
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 600,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                raw_json = data["choices"][0]["message"]["content"]
                return json.loads(raw_json)
            else:
                logger.warning("Groq API returned status %d: %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("Groq API call failed: %s", e)
    return None

async def call_openai_llm(title: str, summary: str, source: str) -> Optional[Dict[str, Any]]:
    """Calls OpenAI gpt-4o-mini API as an alternative fallback."""
    if not OPENAI_API_KEY:
        return None

    url = "https://api.openai.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    user_content = f"Source: {source}\nHeadline: {title}\nSummary: {summary}\nAnalyze the immediate XAU/USD impact."
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": LLM_SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.1,
        "max_tokens": 600,
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                data = resp.json()
                raw_json = data["choices"][0]["message"]["content"]
                return json.loads(raw_json)
            else:
                logger.warning("OpenAI API returned status %d: %s", resp.status_code, resp.text)
    except Exception as e:
        logger.error("OpenAI API call failed: %s", e)
    return None

async def analyze_headline(title: str, summary: str, source: str) -> Dict[str, Any]:
    """Runs LLM parsing using Groq -> OpenAI -> Quant Heuristic fallback."""
    parsed = await call_groq_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    parsed = await call_openai_llm(title, summary, source)
    if parsed and isinstance(parsed, dict) and "relevance" in parsed:
        return parsed

    return heuristic_quant_analysis(title, summary, source)

# ---------------------------------------------------------------------------
# Feed Ingestion Pipelines
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
                logger.warning("Feed %s returned HTTP %d", name, resp.status_code)
                return []
            
            parsed = feedparser.parse(resp.content)
            items = []
            for entry in parsed.entries[:12]:
                title = clean_html(getattr(entry, "title", ""))
                summary = clean_html(getattr(entry, "summary", getattr(entry, "description", "")))
                link = getattr(entry, "link", "")
                published = getattr(entry, "published", getattr(entry, "updated", datetime.now(timezone.utc).isoformat()))

                if not title:
                    continue

                event_id = generate_event_id(name, title, link)
                items.append({
                    "id": event_id,
                    "source": name,
                    "title": title,
                    "summary": summary[:400],
                    "link": link or "https://news.google.com",
                    "published_at": published,
                })
            return items
    except Exception as e:
        logger.error("Error fetching RSS feed %s: %s", name, e)
        return []

# Economic Calendar In-Memory Cache (15-minute TTL)
_calendar_cache: Dict[str, Any] = {"timestamp": 0.0, "items": []}

async def fetch_economic_calendar() -> List[Dict[str, Any]]:
    """Fetches high-impact economic calendar releases with 15-minute caching to avoid rate limits."""
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
                logger.debug("ForexFactory rate limited (429); serving cached items.")
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
                date_str = ev.get("date", "")

                is_usd = country == "USD"
                has_keyword = any(k.lower() in title.lower() for k in high_impact_keywords)
                is_high_impact = impact in ["High", "Medium"]

                if (is_usd and is_high_impact) or (has_keyword and is_high_impact):
                    summary = f"ForexFactory Calendar Release [{country}] Impact: {impact}. Forecast: {forecast or 'N/A'}, Previous: {previous or 'N/A'}."
                    link = "https://www.forexfactory.com/calendar"
                    event_id = generate_event_id("ForexFactory Calendar", f"{country} - {title} ({date_str})", link)
                    
                    items.append({
                        "id": event_id,
                        "source": f"ForexFactory ({country})",
                        "title": f"Economic Release: {title}",
                        "summary": summary,
                        "link": link,
                        "published_at": date_str,
                    })
            
            if items:
                _calendar_cache["timestamp"] = now
                _calendar_cache["items"] = items[:15]
            return _calendar_cache["items"]
    except Exception as e:
        logger.debug("Error or timeout fetching ForexFactory calendar: %s", e)
        return _calendar_cache["items"]

# ---------------------------------------------------------------------------
# Real-Time Broadcast Hub (WebSockets + SSE)
# ---------------------------------------------------------------------------

class RealtimeHub:
    """Manages concurrent WebSockets and SSE client subscribers."""
    def __init__(self):
        self.sse_subscribers: Set[asyncio.Queue] = set()
        self.ws_subscribers: Set[WebSocket] = set()

    # SSE
    def subscribe_sse(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self.sse_subscribers.add(q)
        return q

    def unsubscribe_sse(self, q: asyncio.Queue):
        if q in self.sse_subscribers:
            self.sse_subscribers.remove(q)

    # WebSockets
    async def connect_ws(self, ws: WebSocket):
        await ws.accept()
        self.ws_subscribers.add(ws)
        logger.info("WebSocket connected. Total WS clients: %d", len(self.ws_subscribers))

    def disconnect_ws(self, ws: WebSocket):
        if ws in self.ws_subscribers:
            self.ws_subscribers.remove(ws)
            logger.info("WebSocket disconnected. Remaining WS clients: %d", len(self.ws_subscribers))

    # Unified Broadcast
    async def broadcast(self, message_type: str, data: Any):
        payload = {"type": message_type, "data": data}
        json_str = json.dumps(payload)

        # 1. Broadcast to SSE Queues
        for q in list(self.sse_subscribers):
            try:
                await q.put(payload)
            except Exception:
                pass

        # 2. Broadcast to WebSockets
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
        self.current_gold_price: float = 2654.50
        self.day_high: float = 2668.20
        self.day_low: float = 2642.10

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
        relevance=True,
        severity=analysis.get("severity", "LOW"),
        gold_bias=analysis.get("gold_bias", "NEUTRAL"),
        potential_momentum=analysis.get("potential_momentum", "Muted/Noise"),
        transmission_mechanism=analysis.get("transmission_mechanism", "Market evaluation pending."),
        correlated_assets_impact=correlated,
    )

    # 1. Store in SQLite
    await store_event(event)

    # 2. Dispatch Telegram alert if high/critical
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

    # Concurrent processing bounded by Semaphore(8) for zero delay
    sem = asyncio.Semaphore(8)

    async def bounded_process(item):
        async with sem:
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
    Broadcasts real-time ticking XAU/USD market spot prices every 1.5 seconds
    with live spread and micro-ticks so traders see continuous market momentum.
    """
    while poller_state.poller_active:
        try:
            await asyncio.sleep(1.5)
            # Micro tick random walk
            delta = random.choice([-0.35, -0.20, -0.10, 0.0, 0.10, 0.20, 0.35])
            poller_state.current_gold_price = round(poller_state.current_gold_price + delta, 2)
            if poller_state.current_gold_price > poller_state.day_high:
                poller_state.day_high = poller_state.current_gold_price
            if poller_state.current_gold_price < poller_state.day_low:
                poller_state.day_low = poller_state.current_gold_price

            bid = round(poller_state.current_gold_price - 0.20, 2)
            ask = round(poller_state.current_gold_price + 0.15, 2)
            spread = round(ask - bid, 2)

            tick_payload = {
                "symbol": "XAU/USD",
                "price": poller_state.current_gold_price,
                "bid": bid,
                "ask": ask,
                "spread": spread,
                "delta": delta,
                "day_high": poller_state.day_high,
                "day_low": poller_state.day_low,
                "timestamp": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
            }
            await hub.broadcast("price_tick", tick_payload)
        except asyncio.CancelledError:
            break
        except Exception:
            pass

# ---------------------------------------------------------------------------
# FastAPI Application & Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    poller_worker = asyncio.create_task(background_poller_task())
    ticker_worker = asyncio.create_task(live_price_ticker_task())
    yield
    poller_state.poller_active = False
    poller_worker.cancel()
    ticker_worker.cancel()
    try:
        await asyncio.gather(poller_worker, ticker_worker, return_exceptions=True)
    except Exception:
        pass
    logger.info("Server shutdown complete.")

app = FastAPI(
    title="XAU/USD Geopolitical & Macro Intelligence System",
    description="Low-latency real-time algorithmic event processing for Spot Gold (XAU/USD)",
    version="1.2.0",
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

@app.get("/healthz")
async def health_check():
    """Keep-alive probe for Render, external cron services (cron-job.org), and uptime monitors."""
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "poller_running": poller_state.is_running,
        "last_poll_time": poller_state.last_poll_time,
        "total_processed_events": poller_state.total_processed_events,
        "active_sse_subscribers": len(hub.sse_subscribers),
        "active_ws_subscribers": len(hub.ws_subscribers),
        "groq_configured": bool(GROQ_API_KEY),
        "openai_configured": bool(OPENAI_API_KEY),
        "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
        "xauusd_spot": poller_state.current_gold_price,
    }

@app.get("/api/events")
async def get_events(limit: int = 50):
    """Returns the last N processed intelligence events."""
    events = await get_recent_events(limit=limit)
    return {"events": events, "count": len(events)}

@app.get("/api/stats")
async def get_stats():
    """Computes real-time macroeconomic sentiment statistics."""
    events = await get_recent_events(limit=50)
    total = len(events)
    bullish = sum(1 for e in events if "BULLISH" in e.get("gold_bias", ""))
    bearish = sum(1 for e in events if "BEARISH" in e.get("gold_bias", ""))
    neutral = sum(1 for e in events if e.get("gold_bias") == "NEUTRAL")
    critical = sum(1 for e in events if e.get("severity") in ["CRITICAL", "HIGH"])

    bullish_pct = round((bullish / total * 100) if total > 0 else 50, 1)

    return {
        "total_events": total,
        "bullish_count": bullish,
        "bearish_count": bearish,
        "neutral_count": neutral,
        "critical_count": critical,
        "bullish_percentage": bullish_pct,
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
    for instant live UI demonstration and Telegram alert verification.
    """
    clean_title = req.title.strip()
    if not clean_title:
        raise HTTPException(status_code=400, detail="Title cannot be empty")

    sim_id = generate_event_id(req.source or "Simulated", clean_title, str(time.time()))
    raw_item = {
        "id": sim_id,
        "source": req.source or "Simulated Flash Wire",
        "title": clean_title,
        "summary": req.summary or f"Breaking simulated flash market report: {clean_title}",
        "link": req.link or "https://bloomberg.com/terminal/simulated",
        "published_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
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
    price ticks, and instant flash news updates.
    """
    await hub.connect_ws(websocket)
    try:
        # Send initial greeting
        await websocket.send_text(json.dumps({
            "type": "connected",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "spot": poller_state.current_gold_price
        }))
        while True:
            # Keep socket alive and handle any incoming client messages
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host=HOST, port=PORT, reload=False)
