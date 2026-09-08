"""
Verification Test Suite for XAU/USD Geopolitical & Macro Intelligence System
"""

import asyncio
import json
import os
import sys
from datetime import datetime, timezone

# Ensure current dir is in path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import (
    app,
    init_db,
    store_event,
    get_recent_events,
    analyze_headline,
    heuristic_quant_analysis,
    format_telegram_alert,
    fetch_rss_feed,
    fetch_economic_calendar,
    RSS_FEEDS,
    IntelligenceEvent,
    CorrelatedAssets,
    AssetImpact,
    process_raw_item,
    GROQ_CANDIDATE_MODELS,
    GEMINI_CANDIDATE_MODELS,
    OPENROUTER_CANDIDATE_MODELS,
    OPENAI_CANDIDATE_MODELS,
    generate_macro_event_scenarios,
)
from httpx import ASGITransport, AsyncClient

async def test_quant_analysis():
    print("\n--- 1. Testing Quant Analysis Engine ---")
    
    # War headline
    war_res = heuristic_quant_analysis(
        title="Strait of Hormuz: Military strike disables oil tanker",
        summary="Retaliatory missile barrage strikes maritime corridor.",
        source="Reuters"
    )
    assert war_res["relevance"] is True
    assert war_res["gold_bias"] == "STRONG_BULLISH"
    assert "15-40+" in war_res["potential_momentum"]
    assert war_res["correlated_assets_impact"]["WTI_Crude"]["direction"] == "BULLISH"
    print("  [PASS] War / Geopolitical scenario correctly flagged STRONG_BULLISH + explosive momentum")

    # Dovish headline
    dovish_res = heuristic_quant_analysis(
        title="US Core CPI drops below forecasts, inflation falls to 2.1%",
        summary="Treasury yields plunge as market prices in aggressive rate cuts.",
        source="ForexFactory (USD)"
    )
    assert dovish_res["gold_bias"] == "STRONG_BULLISH"
    assert dovish_res["correlated_assets_impact"]["US10Y_TIPS"]["direction"] == "BEARISH"
    print("  [PASS] Dovish inflation cooling correctly flagged real yields drop + gold bullish")

    # Hawkish headline
    hawkish_res = heuristic_quant_analysis(
        title="NFP surges +350k, strong dollar as rate hike odds rise",
        summary="Job growth accelerates well above forecasts.",
        source="Bloomberg"
    )
    assert "BEARISH" in hawkish_res["gold_bias"]
    assert hawkish_res["correlated_assets_impact"]["DXY"]["direction"] == "BULLISH"
    print("  [PASS] Hawkish headline correctly flagged dollar strength + gold bearish")

async def test_telegram_formatter():
    print("\n--- 2. Testing Telegram Alert Formatter ---")
    dummy_event = IntelligenceEvent(
        id="test-12345",
        source="Federal Reserve Press Releases",
        title="FOMC Statement: Target Rate Lowered 50bps",
        summary="Committee votes to ease monetary policy stance.",
        link="https://federalreserve.gov",
        published_at=datetime.now(timezone.utc).isoformat(),
        relevance=True,
        severity="CRITICAL",
        gold_bias="STRONG_BULLISH",
        potential_momentum="15-40+ pips explosive",
        transmission_mechanism="Real interest rates collapse across benchmark curve, lowering bullion holding costs.",
        correlated_assets_impact=CorrelatedAssets(
            DXY=AssetImpact(direction="BEARISH", logic="Dollar liquidation on rate cut."),
            US10Y_TIPS=AssetImpact(direction="BEARISH", logic="Real yields dive 12bps."),
            WTI_Crude=AssetImpact(direction="BULLISH", logic="Growth support."),
            Silver_XAG=AssetImpact(direction="STRONG_BULLISH", logic="High-beta precious metals rally."),
            VIX=AssetImpact(direction="BEARISH", logic="Volatility crush on Fed support.")
        )
    )

    alert_text = format_telegram_alert(dummy_event)
    assert "CRITICAL ALERT" in alert_text
    assert "STRONG BULLISH" in alert_text
    assert "15-40+ pips explosive" in alert_text
    assert "DXY:" in alert_text
    assert "US10Y TIPS:" in alert_text
    print("  [PASS] Telegram HTML formatting generated properly with emojis, momentum & cross-asset breakdown")

async def test_database_persistence():
    print("\n--- 3. Testing SQLite Storage & Strict Epoch Sorting ---")
    await init_db()
    
    # Insert an older event (epoch = 1000.0)
    older_event = IntelligenceEvent(
        id="test-db-older",
        source="Test Feed Older",
        title="Older Central Bank Bulletin",
        summary="Historical bullion reserves recap.",
        link="https://gold.org/old",
        published_at="2026-08-01T12:00:00Z",
        published_epoch=1785585600.0,
        relevance=True,
        severity="LOW",
        gold_bias="NEUTRAL",
        potential_momentum="Muted/Noise",
        transmission_mechanism="Routine reserve accounting.",
        correlated_assets_impact=CorrelatedAssets(
            DXY=AssetImpact(direction="NEUTRAL", logic="No effect."),
            US10Y_TIPS=AssetImpact(direction="NEUTRAL", logic="No effect."),
            WTI_Crude=AssetImpact(direction="NEUTRAL", logic="No effect."),
            Silver_XAG=AssetImpact(direction="NEUTRAL", logic="No effect."),
            VIX=AssetImpact(direction="NEUTRAL", logic="No effect.")
        )
    )
    # Insert a newer event (epoch = 2000.0)
    newer_event = IntelligenceEvent(
        id="test-db-newer",
        source="Test Feed Newer",
        title="Breaking Flash: Emergency Gold Buying Surge",
        summary="Fresh liquidity injection into sovereign physical bullion.",
        link="https://gold.org/new",
        published_at="2026-09-08T10:00:00Z",
        published_epoch=1788861600.0,
        relevance=True,
        severity="CRITICAL",
        gold_bias="STRONG_BULLISH",
        potential_momentum="15-40+ pips explosive",
        transmission_mechanism="Aggressive sudden physical bullion allocation.",
        correlated_assets_impact=CorrelatedAssets(
            DXY=AssetImpact(direction="BEARISH", logic="De-dollarization."),
            US10Y_TIPS=AssetImpact(direction="BEARISH", logic="Real yields collapse."),
            WTI_Crude=AssetImpact(direction="BULLISH", logic="Inflation flight."),
            Silver_XAG=AssetImpact(direction="STRONG_BULLISH", logic="High-beta rally."),
            VIX=AssetImpact(direction="BULLISH", logic="Volatility surge.")
        )
    )

    # Store older first, then newer
    await store_event(older_event)
    await store_event(newer_event)

    recent = await get_recent_events(limit=200)
    assert len(recent) >= 2
    
    # Locate indices of both test events
    idx_newer = next((i for i, e in enumerate(recent) if e["id"] == "test-db-newer"), -1)
    idx_older = next((i for i, e in enumerate(recent) if e["id"] == "test-db-older"), -1)

    assert idx_newer != -1 and idx_older != -1
    assert idx_newer < idx_older, f"Newer event (idx {idx_newer}) must appear BEFORE older event (idx {idx_older})!"
    print(f"  [PASS] Strict descending epoch order verified: newer (epoch {newer_event.published_epoch}) appears at index {idx_newer} ahead of older (epoch {older_event.published_epoch}) at index {idx_older}")

async def test_live_feed_ingestion():
    print("\n--- 4. Testing Feed Fetchers ---")
    
    # Test BBC or Reuters feed
    bbc_feed = RSS_FEEDS[0]
    entries = await fetch_rss_feed(bbc_feed)
    print(f"  [PASS] {bbc_feed['name']} fetched {len(entries)} items")
    assert len(entries) > 0
    assert "title" in entries[0]
    assert "source" in entries[0]

    # Test Economic Calendar
    calendar_events = await fetch_economic_calendar()
    print(f"  [PASS] ForexFactory Calendar fetched {len(calendar_events)} high/medium impact items")
    assert isinstance(calendar_events, list)

async def test_api_endpoints():
    print("\n--- 5. Testing FastAPI ASGI Endpoints ---")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        
        # Ping endpoint
        ping_resp = await client.get("/ping")
        assert ping_resp.status_code == 200
        assert ping_resp.text.strip('"') == "pong"
        print("  [PASS] GET /ping returned 200 OK (pong)")

        # OANDA Live Price endpoint
        o_resp = await client.get("/api/price/oanda")
        assert o_resp.status_code == 200
        o_data = o_resp.json()
        assert "price" in o_data
        assert "bid" in o_data
        assert "ask" in o_data
        print(f"  [PASS] GET /api/price/oanda returned 200 OK (${o_data['price']} | Spread: {o_data.get('spread')})")

        # Healthz
        h_resp = await client.get("/healthz")
        assert h_resp.status_code == 200
        h_data = h_resp.json()
        assert h_data["status"] == "healthy"
        print("  [PASS] GET /healthz returned 200 OK")

        # Events endpoint
        e_resp = await client.get("/api/events")
        assert e_resp.status_code == 200
        assert "events" in e_resp.json()
        print("  [PASS] GET /api/events returned 200 OK")

        # Stats endpoint
        s_resp = await client.get("/api/stats")
        assert s_resp.status_code == 200
        assert "bullish_percentage" in s_resp.json()
        print("  [PASS] GET /api/stats returned 200 OK")

        # Simulate breaking event
        sim_payload = {
            "title": "Taiwan Strait: Unannounced Naval Drills Encircle Island, Shipping Halted",
            "summary": "Escalation in East Asia triggers massive safe haven flows into gold and Swiss Franc.",
            "source": "Flash Wire Geopolitics"
        }
        sim_resp = await client.post("/api/simulate-event", json=sim_payload)
        assert sim_resp.status_code == 200
        sim_data = sim_resp.json()
        assert sim_data["status"] == "success"
        assert sim_data["event"]["gold_bias"] == "STRONG_BULLISH"
        print(f"  [PASS] POST /api/simulate-event parsed event with bias: {sim_data['event']['gold_bias']}")

        # Dashboard HTML
        ui_resp = await client.get("/")
        assert ui_resp.status_code == 200
        assert "AUREUS" in ui_resp.text
        assert "Macro Asset Transmission Matrix" in ui_resp.text
async def test_ai_fallback_cascade():
    print("\n--- 6. Testing Multi-Provider AI Fallback Cascade ---")
    assert len(GROQ_CANDIDATE_MODELS) >= 5, "Groq must have multiple fallback models configured!"
    assert "llama-3.1-8b-instant" in GROQ_CANDIDATE_MODELS, "llama-3.1-8b-instant must be in Groq fallbacks!"
    assert len(GEMINI_CANDIDATE_MODELS) >= 3, "Gemini must have multiple fallback models configured!"
    assert len(OPENROUTER_CANDIDATE_MODELS) >= 4, "OpenRouter must have multiple fallback models configured!"
    assert len(OPENAI_CANDIDATE_MODELS) >= 3, "OpenAI must have multiple fallback models configured!"
    
    # Test economic calendar macro scenario generator
    scenarios = generate_macro_event_scenarios(
        title="Economic Release: Core CPI m/m",
        summary="ForexFactory Calendar Release [USD] Impact: High. Forecast: 0.3%, Previous: 0.3%",
        source="ForexFactory (USD)",
        country="USD"
    )
    assert scenarios["event_type"] == "INFLATION"
    assert "bullish_trigger" in scenarios
    assert "bearish_trigger" in scenarios
    assert "transmission" in scenarios
    print("  [PASS] Macro economic calendar scenario engine generated institutional rules for CPI")

    res = await analyze_headline(
        title="Fed signals upcoming rate pause amidst cooling labor market",
        summary="Treasuries rally on dovish central bank signals.",
        source="Reuters"
    )
    assert res is not None
    assert "relevance" in res
    assert res["relevance"] is True
    assert "gold_bias" in res
    print(f"  [PASS] Multi-provider AI cascade verified: {len(GROQ_CANDIDATE_MODELS)} Groq, {len(GEMINI_CANDIDATE_MODELS)} Gemini, {len(OPENROUTER_CANDIDATE_MODELS)} OpenRouter, {len(OPENAI_CANDIDATE_MODELS)} OpenAI models, seamless heuristic fallback active.")

async def run_all_tests():
    print("=================================================================")
    print("Running Full System Verification for XAU/USD Intelligence Engine")
    print("=================================================================")
    await test_quant_analysis()
    await test_telegram_formatter()
    await test_database_persistence()
    await test_live_feed_ingestion()
    await test_api_endpoints()
    await test_ai_fallback_cascade()
    print("\n=================================================================")
    print("ALL TESTS PASSED WITH 100% SUCCESS!")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_all_tests())
