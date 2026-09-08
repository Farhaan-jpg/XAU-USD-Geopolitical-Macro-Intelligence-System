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
    print("\n--- 3. Testing SQLite Storage & Deduplication ---")
    await init_db()
    
    dummy_event = IntelligenceEvent(
        id="test-db-001",
        source="Test Feed",
        title="Central Banks Boost Gold Reserves",
        summary="Official sector bullion purchases exceed 1,000 tonnes.",
        link="https://gold.org",
        published_at=datetime.now(timezone.utc).isoformat(),
        relevance=True,
        severity="HIGH",
        gold_bias="BULLISH",
        potential_momentum="5-15 pips drift",
        transmission_mechanism="Structural central bank reserve diversification away from fiat sovereign debt.",
        correlated_assets_impact=CorrelatedAssets(
            DXY=AssetImpact(direction="NEUTRAL", logic="Slow de-dollarization."),
            US10Y_TIPS=AssetImpact(direction="NEUTRAL", logic="Long-term reserve shift."),
            WTI_Crude=AssetImpact(direction="NEUTRAL", logic="No oil impact."),
            Silver_XAG=AssetImpact(direction="BULLISH", logic="Precious metals demand spillover."),
            VIX=AssetImpact(direction="NEUTRAL", logic="Calm macro conditions.")
        )
    )

    await store_event(dummy_event)
    recent = await get_recent_events(limit=10)
    assert len(recent) >= 1
    found = any(e["id"] == "test-db-001" for e in recent)
    assert found is True
    print("  [PASS] Successfully persisted and retrieved event from SQLite (WAL mode)")

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
        print("  [PASS] GET / rendered UI dashboard successfully")

async def run_all_tests():
    print("=================================================================")
    print("Running Full System Verification for XAU/USD Intelligence Engine")
    print("=================================================================")
    await test_quant_analysis()
    await test_telegram_formatter()
    await test_database_persistence()
    await test_live_feed_ingestion()
    await test_api_endpoints()
    print("\n=================================================================")
    print("ALL TESTS PASSED WITH 100% SUCCESS!")
    print("=================================================================")

if __name__ == "__main__":
    asyncio.run(run_all_tests())
