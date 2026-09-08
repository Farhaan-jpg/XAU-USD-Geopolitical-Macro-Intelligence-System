# AUREUS // XAU/USD Geopolitical & Macro Intelligence System

A production-grade, low-latency event-driven algorithmic intelligence system designed for Gold (XAU/USD) quant desks, macro traders, and algorithmic execution engines.

---

## Key Architecture & Features

1. **High-Frequency Ingestion Pipelines**:
   - **Financial & World RSS Feeds**: BBC World News, Al Jazeera English, Reuters Markets Wire, NYT World News, Federal Reserve Press Releases.
   - **Economic Calendar**: ForexFactory calendar JSON parsing with automatic impact filtering (CPI, PPI, NFP, FOMC, Fed Chair Powell, Core PCE).
   - Rate-limiting protection with in-memory TTL caching and deduplication via deterministic SHA-256 event IDs.

2. **Institutional Quantitative LLM Parsing Engine**:
   - Primary: **Groq Llama-3.3-70B** (`llama-3.3-70b-versatile`) with strict JSON mode schema enforcement.
   - Secondary: **OpenAI** (`gpt-4o-mini`) fallback.
   - Built-in **Deterministic Quant Heuristic Engine** allowing 100% functionality and testability out-of-the-box even prior to inputting API keys.
   - Returns:
     - `relevance` (boolean)
     - `severity` (`CRITICAL` | `HIGH` | `MEDIUM` | `LOW`)
     - `gold_bias` (`STRONG_BULLISH` | `BULLISH` | `NEUTRAL` | `BEARISH` | `STRONG_BEARISH`)
     - `potential_momentum` (`15-40+ pips explosive` | `5-15 pips drift` | `Muted/Noise`)
     - `transmission_mechanism` (mechanical 1-2 sentence real yield / macro reasoning)
     - `correlated_assets_impact`: Directional bias and logic for **DXY**, **US10Y TIPS (Real Yields)**, **WTI Crude Oil**, **Silver (XAG/USD)**, and **VIX**.

3. **Telegram Bot Dispatcher**:
   - Instant rich HTML notifications with emoji badges (`🚨 [CRITICAL]`, `🟢 STRONG BULLISH`), expected momentum gauge, transmission mechanism, and correlated asset matrix.
   - Asynchronous non-blocking dispatch with graceful bypass when unconfigured.

4. **Persistence & Performance**:
   - **SQLite Database** with Write-Ahead Logging (`PRAGMA journal_mode=WAL;`) for zero-lock concurrency.
   - Stores all processed events, deduplicating repetitive headlines across feeds.
   - Keeps and serves the last 50 processed events via REST.

5. **Real-Time Single-Page Dashboard**:
   - Sleek **Obsidian / Zinc dark mode** (`#09090b`), gold metallic accents (`#D4AF37`), glassmorphism, and smooth CSS animations.
   - **Server-Sent Events (SSE)** broadcast: Push updates without page reloads.
   - **Live Macro Correlation Matrix**: Dynamic cross-asset tracking widgets.
   - **Web Audio Alert Chimes**: Synthesized acoustic radar bells on critical/high impact events (no external MP3 dependencies).
   - Instant Search & Multi-Tag Filtering (All, Critical, Bullish Only, Bearish Only, Geopolitics).
   - Interactive **Flash Scenario Simulator**: Inject real-time geopolitical crises (e.g. Strait of Hormuz tanker strikes, surprise 50bps rate cuts) to test the engine on demand.
   - Built-in `/healthz` keep-alive endpoint for external uptime monitors.

---

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Environment Configuration (Optional)
Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```
Fill in your keys:
```ini
# LLM Providers (Groq recommended for high-speed Llama-3.3-70B)
GROQ_API_KEY=gsk_your_groq_api_key_here
OPENAI_API_KEY=

# Telegram Alerts (Optional)
TELEGRAM_BOT_TOKEN=123456789:ABCdefGhIJKlmNoPQRstuVWXyz
TELEGRAM_CHAT_ID=@your_channel_or_chat_id

# Poller Interval (Default: 25 seconds)
POLL_INTERVAL_SECONDS=25
```
*Note: If no API keys are provided, the system automatically runs the built-in Quantitative Heuristic Engine with full accuracy.*

### 3. Run the Server
```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --reload
```
Open your browser at:
```
http://localhost:8000
```

---

## Verification & Automated Testing

Run the comprehensive test suite:
```bash
python test_system.py
```
This tests:
- Quant analysis engine across war, dovish inflation, and hawkish data scenarios.
- Telegram HTML alert generator.
- SQLite WAL mode database persistence and deduplication.
- Live RSS and economic calendar ingestion feeds.
- FastAPI routes: `/healthz`, `/api/events`, `/api/stats`, `/api/simulate-event`, and `/`.

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/` | Renders the real-time financial intelligence dashboard |
| `GET` | `/healthz` | Uptime probe returning poller status, subscriber count, and active providers |
| `GET` | `/api/events?limit=50` | Retrieves the last N parsed events from SQLite |
| `GET` | `/api/stats` | Returns real-time sentiment distribution (Bullish/Bearish ratio, critical alert counts) |
| `GET` | `/api/events/stream` | Server-Sent Events (SSE) live push stream |
| `POST` | `/api/trigger-poll` | Manually triggers an immediate feed ingestion cycle |
| `POST` | `/api/simulate-event` | Injects a breaking headline for instant parsing and live broadcast |

---

## Core Quantitative Pricing Formulas

1. **Real Interest Rates ($r = y - \pi$)**:
   $$\text{Real Yield} = \text{Nominal 10Y Treasury Yield} - \text{10Y Breakeven Inflation}$$
   Gold is a non-yielding asset; its opportunity cost is determined by US 10-year real TIPS yields with a fundamental correlation of approximately $-0.85$.

2. **US Dollar Index (DXY)**:
   Since spot gold is denominated in USD ($/troy oz), dollar debasement directly inflates dollar-based bullion prices (correlation $-0.80$).

3. **Geopolitical Risk Premia**:
   Sudden military escalations (e.g. Strait of Hormuz, Taiwan Strait, Middle East strikes) drive instantaneous flights to safety, decoupling gold from nominal yields.
