"""
Dynamic Liquidity HeatMap Profile [Pro System] Engine for XAU/USD
Accurately reproduces TradingView Pine Script v6 logic:
- Dynamic ATR(5)/50 & 10-bar Volume Normalization
- High-resolution (100-step) Liquidity Pivot Tracker
- Dynamic Liquidation/Breach Invalidation Filter
- 50-bin Volume Profile & Point of Control (POC)
- Dual-MA (50/200 EMA) Trend Confirmation Filter
- APEX (100%), Major Entry Zones (>= 85%), HQ Pullback Zones (65% - 84%)
- Real-time Zone Radar & Proximity Alerts (Nearing / Entered)
"""

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("LiquidityEngine")

# Configuration Defaults (matching Pine Script inputs)
DEFAULT_LOOKBACK = 300
DEFAULT_BINS = 50
DEFAULT_RESOLUTION = 100
DEFAULT_EZ_THRESHOLD = 0.85
DEFAULT_PB_THRESHOLD = 0.65
DEFAULT_FAST_MA = 50
DEFAULT_SLOW_MA = 200
DEFAULT_PROXIMITY_BUFFER_USD = float(os.getenv("ZONE_PROXIMITY_BUFFER_USD", "2.0"))


@dataclass
class CandleBar:
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class Pivot:
    value: float
    index: int
    volume_: float
    vol: float
    is_lower: bool


@dataclass
class LiquidityZone:
    id: str
    bin_index: int
    lower: float
    upper: float
    mid: float
    volume: float
    volume_pct: float  # Percentage of maxBinVol (0.0 to 100.0)
    is_lower: bool     # True if below current price (Support/Long), False if above (Resistance/Short)
    is_major: bool
    is_apex: bool
    is_pullback: bool
    zone_type: str     # e.g., "Pro Continuation Long", "HQ Pullback Short", "APEX ZONE"
    signal: str        # "BUY", "SELL", "REVERSAL_BUY", "REVERSAL_SELL", "RANGE_BUY", "RANGE_SELL"
    sl_price: float    # Invalidation / Stop Loss price
    start_bar: int     # Bar index where zone originated
    box_color: str
    border_color: str


@dataclass
class HeatmapBin:
    index: int
    lower: float
    upper: float
    mid: float
    volume: float
    value_vol: float   # 0 to 50 scale as in Pine Script
    is_poc: bool
    is_major: bool
    is_pullback: bool
    color: str


@dataclass
class LiquidityProfileResult:
    timestamp: str
    symbol: str
    current_price: float
    lookback: int
    bins_count: int
    trend_state: int       # 1: Uptrend, -1: Downtrend, 0: Ranging
    trend_label: str       # "Bullish Continuation", "Bearish Continuation", "Ranging / Chop"
    fast_ma: float
    slow_ma: float
    atr: float
    top_boundary: float
    bot_boundary: float
    poc_price: float
    poc_volume: float
    apex_zone: Optional[LiquidityZone]
    major_zones: List[LiquidityZone]
    pullback_zones: List[LiquidityZone]
    all_active_zones: List[LiquidityZone]
    heatmap_bins: List[HeatmapBin]
    nearest_support: Optional[LiquidityZone]
    nearest_resistance: Optional[LiquidityZone]
    distance_to_nearest_support: Optional[float]
    distance_to_nearest_resistance: Optional[float]


# ---------------------------------------------------------------------------
# Technical Indicator Calculations (Pure Python & Robust)
# ---------------------------------------------------------------------------

def calculate_ema(values: List[float], length: int) -> List[float]:
    """Calculates Exponential Moving Average matching Pine Script ta.ema."""
    if not values:
        return []
    if len(values) < length:
        avg = sum(values) / len(values)
        return [avg] * len(values)

    alpha = 2.0 / (length + 1)
    ema = [0.0] * len(values)
    seed = sum(values[:length]) / length
    for i in range(length):
        ema[i] = seed

    for i in range(length, len(values)):
        ema[i] = alpha * values[i] + (1.0 - alpha) * ema[i - 1]

    return ema


def calculate_atr(bars: List[CandleBar], length: int = 5) -> List[float]:
    """Calculates Average True Range matching Pine Script ta.atr(length)."""
    n = len(bars)
    if n == 0:
        return []
    if n == 1:
        return [bars[0].high - bars[0].low]

    tr = [0.0] * n
    tr[0] = bars[0].high - bars[0].low
    for i in range(1, n):
        hl = bars[i].high - bars[i].low
        hc = abs(bars[i].high - bars[i - 1].close)
        lc = abs(bars[i].low - bars[i - 1].close)
        tr[i] = max(hl, hc, lc)

    # Pine Script ta.atr uses RMA (exponential moving average with alpha = 1 / length)
    atr = [0.0] * n
    atr[0] = tr[0]
    for i in range(1, n):
        atr[i] = (atr[i - 1] * (length - 1) + tr[i]) / length

    return atr


# ---------------------------------------------------------------------------
# Candle Ingestion Engine
# ---------------------------------------------------------------------------

async def fetch_ohlcv_bars(
    symbol: str = "XAU/USD",
    lookback: int = DEFAULT_LOOKBACK,
    timeframe: str = "15m",
    oanda_api_key: str = "",
    oanda_account_id: str = "",
    oanda_env: str = "practice",
    current_spot_price: Optional[float] = None,
) -> List[CandleBar]:
    """
    Fetches real historical OHLCV bars for Spot Gold (XAU/USD).
    Priority 1: Official OANDA v20 API if credentials provided.
    Priority 2: Binance PAXGUSDT (Paxos 1:1 Spot Gold, sub-second & keyless).
    Priority 3: Yahoo Finance GC=F (COMEX Gold Futures).
    Zero-latency synchronization: calibrates the latest candle with current_spot_price.
    """
    bars: List[CandleBar] = []

    # 1. Official OANDA v20 API
    if oanda_api_key and oanda_account_id:
        granularity_map = {"1m": "M1", "5m": "M5", "15m": "M15", "1h": "H1", "4h": "H4", "1d": "D"}
        gran = granularity_map.get(timeframe, "M15")
        base_url = "https://api-fxtrade.oanda.com" if oanda_env == "live" else "https://api-fxpractice.oanda.com"
        url = f"{base_url}/v3/instruments/XAU_USD/candles?count={lookback}&granularity={gran}&price=M"
        headers = {"Authorization": f"Bearer {oanda_api_key}", "Content-Type": "application/json"}
        try:
            async with httpx.AsyncClient(timeout=6.0) as client:
                r = await client.get(url, headers=headers)
                if r.status_code == 200:
                    data = r.json()
                    candles = data.get("candles", [])
                    for c in candles:
                        mid = c.get("mid", {})
                        ts = datetime.fromisoformat(c["time"].replace("Z", "+00:00")).timestamp()
                        bars.append(CandleBar(
                            timestamp=ts,
                            open=float(mid.get("o", 0.0)),
                            high=float(mid.get("h", 0.0)),
                            low=float(mid.get("l", 0.0)),
                            close=float(mid.get("c", 0.0)),
                            volume=float(c.get("volume", 100.0)),
                        ))
                    if len(bars) >= 50:
                        logger.debug("Successfully fetched %d bars from OANDA v20 REST", len(bars))
                        return _finalize_bars(bars, current_spot_price)
        except Exception as e:
            logger.debug("OANDA v20 candles failed: %s, trying Binance PAXGUSDT...", e)

    # 2. Binance PAXGUSDT (Paxos Gold, 100% asset-backed Spot Gold, trades at exact XAU/USD parity)
    binance_interval_map = {"1m": "1m", "5m": "5m", "15m": "15m", "1h": "1h", "4h": "4h", "1d": "1d"}
    interval = binance_interval_map.get(timeframe, "15m")
    url = f"https://api.binance.com/api/v3/klines?symbol=PAXGUSDT&interval={interval}&limit={min(lookback + 50, 1000)}"
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            if resp.status_code == 200:
                klines = resp.json()
                for k in klines:
                    bars.append(CandleBar(
                        timestamp=float(k[0]) / 1000.0,
                        open=float(k[1]),
                        high=float(k[2]),
                        low=float(k[3]),
                        close=float(k[4]),
                        volume=float(k[5]),
                    ))
                if len(bars) >= 50:
                    logger.debug("Successfully fetched %d bars from Binance PAXGUSDT", len(bars))
                    return _finalize_bars(bars, current_spot_price)
    except Exception as e:
        logger.debug("Binance PAXGUSDT candles failed: %s, trying Yahoo Finance...", e)

    # 3. Yahoo Finance COMEX Gold Futures (GC=F)
    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            r = await client.get(
                "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?interval=15m&range=5d",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            if r.status_code == 200:
                d = r.json()["chart"]["result"][0]
                timestamps = d.get("timestamp", [])
                q = d["indicators"]["quote"][0]
                opens = q.get("open", [])
                highs = q.get("high", [])
                lows = q.get("low", [])
                closes = q.get("close", [])
                volumes = q.get("volume", [])
                for i in range(len(timestamps)):
                    if closes[i] is not None and highs[i] is not None and lows[i] is not None:
                        bars.append(CandleBar(
                            timestamp=float(timestamps[i]),
                            open=float(opens[i] or closes[i]),
                            high=float(highs[i]),
                            low=float(lows[i]),
                            close=float(closes[i]),
                            volume=float(volumes[i] or 100.0),
                        ))
                if len(bars) >= 50:
                    logger.debug("Successfully fetched %d bars from Yahoo GC=F", len(bars))
                    return _finalize_bars(bars, current_spot_price)
    except Exception as e:
        logger.warning("Yahoo Finance candles failed: %s", e)

    # 4. Synthesize graceful fallback bars if network is unreachable
    if not bars:
        logger.warning("Network feeds unreachable. Synthesizing baseline bars around current price.")
        base = current_spot_price or 4390.0
        now_ts = time.time()
        for i in range(lookback):
            t = now_ts - (lookback - i) * 900
            drift = math.sin(i / 15.0) * 12.0 + (i / 300.0) * 8.0
            p = base + drift
            bars.append(CandleBar(
                timestamp=t,
                open=round(p - 1.0, 2),
                high=round(p + 2.5, 2),
                low=round(p - 2.5, 2),
                close=round(p, 2),
                volume=round(150.0 + abs(drift) * 10.0, 2),
            ))

    return _finalize_bars(bars, current_spot_price)


def _finalize_bars(bars: List[CandleBar], current_spot_price: Optional[float]) -> List[CandleBar]:
    """Sorts bars chronologically and optionally syncs the final bar to live spot quote."""
    bars.sort(key=lambda b: b.timestamp)
    if current_spot_price and bars:
        last = bars[-1]
        bars[-1] = CandleBar(
            timestamp=last.timestamp,
            open=last.open,
            high=max(last.high, current_spot_price),
            low=min(last.low, current_spot_price),
            close=current_spot_price,
            volume=last.volume,
        )
    return bars


# ---------------------------------------------------------------------------
# Pine Script Liquidity HeatMap Profile Algorithm
# ---------------------------------------------------------------------------

class LiquidityEngine:
    """
    Mathematical reproduction of:
    indicator("Dynamic Liquidity HeatMap Profile [Pro System]", overlay = true)
    """

    def __init__(
        self,
        lookback: int = DEFAULT_LOOKBACK,
        bins: int = DEFAULT_BINS,
        resolution: int = DEFAULT_RESOLUTION,
        ez_threshold: float = DEFAULT_EZ_THRESHOLD,
        pb_threshold: float = DEFAULT_PB_THRESHOLD,
        fast_ma_length: int = DEFAULT_FAST_MA,
        slow_ma_length: int = DEFAULT_SLOW_MA,
        use_trend_filter: bool = True,
    ):
        self.lookback = lookback
        self.bins = bins
        self.resolution = resolution
        self.ez_threshold = ez_threshold
        self.pb_threshold = pb_threshold
        self.fast_ma_length = fast_ma_length
        self.slow_ma_length = slow_ma_length
        self.use_trend_filter = use_trend_filter

    def compute(self, bars: List[CandleBar], current_spot_price: Optional[float] = None) -> LiquidityProfileResult:
        """
        Executes the Pine Script algorithm across the provided OHLCV series.
        """
        n_bars = len(bars)
        if n_bars < 20:
            raise ValueError(f"Insufficient bars for profile calculation: {n_bars} < 20")

        # Effective lookback
        lb = min(self.lookback, n_bars)
        active_bars = bars[-lb:]
        current_close = current_spot_price if current_spot_price is not None else active_bars[-1].close

        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        volumes = [b.volume for b in bars]

        # 1. Dual-MA Trend Filter
        fast_ma_series = calculate_ema(closes, self.fast_ma_length)
        slow_ma_series = calculate_ema(closes, self.slow_ma_length)
        fast_ma = fast_ma_series[-1]
        slow_ma = slow_ma_series[-1]

        # trendState: 1 = Uptrend, -1 = Downtrend, 0 = Ranging/Chop
        if fast_ma > slow_ma and current_close > slow_ma:
            trend_state = 1
            trend_label = "Bullish Continuation (EMA 50 above EMA 200, Price above EMA 200)"
        elif fast_ma < slow_ma and current_close < slow_ma:
            trend_state = -1
            trend_label = "Bearish Continuation (EMA 50 below EMA 200, Price below EMA 200)"
        else:
            trend_state = 0
            trend_label = "Ranging / Consolidation"

        # 2. 10-bar Volume Sum & Normalization
        vol_sums = [0.0] * n_bars
        for i in range(n_bars):
            start_idx = max(0, i - 9)
            vol_sums[i] = sum(volumes[start_idx : i + 1])

        # maxVol over lookBack
        active_vol_sums = vol_sums[-lb:]
        max_vol = max(active_vol_sums) if active_vol_sums else 1.0

        n_vol_series = [
            (vol_sums[i] / max_vol * 100.0) if max_vol > 0 else 0.0
            for i in range(n_bars)
        ]

        # 3. ATR(5) / 50 and dynamic offset
        atr_series = calculate_atr(bars, length=5)
        scaled_atr = [atr / 50.0 for atr in atr_series]

        atr_nvol = [scaled_atr[i] * n_vol_series[i] for i in range(n_bars)]
        active_atr_nvol = atr_nvol[-lb:]
        offset = max(active_atr_nvol) if active_atr_nvol else 0.5

        # 4. Top and Bot Dynamic Boundaries
        # top = ta.highest(high + offset, lookBack), bot = ta.lowest(low - offset, lookBack)
        active_highs = highs[-lb:]
        active_lows = lows[-lb:]
        top = max(active_highs) + offset
        bot = min(active_lows) - offset
        if top <= bot:
            top = bot + 10.0

        # 5. High-Resolution (100-step) Liquidity Pivot Tracker
        step_res = (top - bot) / self.resolution
        pivots: List[Pivot] = []

        start_bar_index = n_bars - lb
        for idx in range(start_bar_index, n_bars):
            # h = ta.highest(high, 2), l = ta.lowest(low, 2)
            prev_idx = max(0, idx - 1)
            h = max(highs[prev_idx], highs[idx])
            l = min(lows[prev_idx], lows[idx])

            level1 = highs[idx] + scaled_atr[idx] * n_vol_series[idx]
            level2 = lows[idx] - scaled_atr[idx] * n_vol_series[idx]

            # High Pivot
            if h == highs[idx]:
                for step_i in range(self.resolution):
                    b_lower = bot + step_res * step_i
                    mid = b_lower + step_res / 2.0
                    if abs(level1 - mid) <= step_res:
                        pivots.append(Pivot(
                            value=mid,
                            index=idx,
                            volume_=n_vol_series[idx],
                            vol=vol_sums[idx],
                            is_lower=False,
                        ))

            # Low Pivot
            if l == lows[idx]:
                for step_i in range(self.resolution):
                    b_lower = bot + step_res * step_i
                    mid = b_lower + step_res / 2.0
                    if abs(level2 - mid) <= step_res:
                        pivots.append(Pivot(
                            value=mid - scaled_atr[idx] * n_vol_series[idx],
                            index=idx,
                            volume_=n_vol_series[idx],
                            vol=vol_sums[idx],
                            is_lower=True,
                        ))

            # Dynamic Invalidation Filter:
            # "if (p.isLower and low < p.value) or (not p.isLower and high > p.value) -> remove"
            bar_low = lows[idx]
            bar_high = highs[idx]
            pivots = [
                p for p in pivots
                if not ((p.is_lower and bar_low < p.value) or (not p.is_lower and bar_high > p.value))
            ]

        # 6. 50-bin Volume HeatMap Profile
        step_bin = (top - bot) / self.bins
        volume_bins = [0.0] * self.bins

        for lvl in pivots:
            vol_ = lvl.vol
            line_y = lvl.value
            for j in range(self.bins):
                b_lower = bot + step_bin * j
                mid = b_lower + step_bin / 2.0
                if abs(line_y - mid) < step_bin:
                    volume_bins[j] += vol_

        max_bin_vol = max(volume_bins) if volume_bins else 0.0

        # Point of Control (POC)
        poc_bin_index = 0
        if max_bin_vol > 0:
            poc_bin_index = volume_bins.index(max_bin_vol)
        poc_lower = bot + step_bin * poc_bin_index
        poc_price = round(poc_lower + step_bin / 2.0, 2)

        # 7. Extract Bins & Classify Zones
        heatmap_bins: List[HeatmapBin] = []
        major_zones: List[LiquidityZone] = []
        pullback_zones: List[LiquidityZone] = []
        apex_zone: Optional[LiquidityZone] = None

        for j in range(self.bins):
            b_lower = round(bot + step_bin * j, 2)
            b_upper = round(b_lower + step_bin, 2)
            mid = round(b_lower + step_bin / 2.0, 2)
            voll = volume_bins[j]
            value_vol = round((voll / max_bin_vol * 50.0) if max_bin_vol > 0 else 0.0, 2)
            vol_pct = round((voll / max_bin_vol * 100.0) if max_bin_vol > 0 else 0.0, 1)

            is_poc = (voll == max_bin_vol and max_bin_vol > 0)
            is_lower = (current_close > mid)  # Support if price above, Resistance if price below

            is_major = (voll >= max_bin_vol * self.ez_threshold and voll > 0) if max_bin_vol > 0 else False
            is_pullback = (
                voll >= max_bin_vol * self.pb_threshold
                and voll < max_bin_vol * self.ez_threshold
                and voll > 0
            ) if max_bin_vol > 0 else False
            is_apex = is_poc

            # Determine Bin Color for UI
            if is_poc:
                bin_color = "#f97316"  # Orange POC
            elif is_major:
                bin_color = "#10b981" if is_lower else "#ef4444"
            elif is_pullback:
                bin_color = "#14b8a6" if is_lower else "#d946ef"
            else:
                bin_color = "#3b82f6" if is_lower else "#64748b"

            heatmap_bins.append(HeatmapBin(
                index=j,
                lower=b_lower,
                upper=b_upper,
                mid=mid,
                volume=round(voll, 2),
                value_vol=value_vol,
                is_poc=is_poc,
                is_major=is_major,
                is_pullback=is_pullback,
                color=bin_color,
            ))

            # Find origin bar for the zone
            start_bar = n_bars - lb
            for i in range(lb):
                bar_i = n_bars - 1 - i
                if is_lower:
                    if lows[bar_i] < mid or i == lb - 1:
                        start_bar = bar_i
                        break
                else:
                    if highs[bar_i] > mid or i == lb - 1:
                        start_bar = bar_i
                        break

            # Classification of Major Zones & HQ Pullback Zones
            if is_major or is_pullback:
                sl_price = b_lower if is_lower else b_upper
                draw_zone = True
                zone_type = ""
                signal = ""
                box_color = ""
                border_color = ""

                if is_major:
                    apex_str = " 🌟 APEX ZONE" if is_apex else ""
                    if self.use_trend_filter:
                        if is_lower:
                            if trend_state == 1:
                                zone_type = f"Pro Continuation Long{apex_str}"
                                signal = "BUY"
                                box_color = "rgba(16, 185, 129, 0.20)"
                                border_color = "#10b981"
                            elif trend_state == -1:
                                zone_type = f"Risk: Reversal Long{apex_str}"
                                signal = "REVERSAL_BUY"
                                box_color = "rgba(249, 115, 22, 0.20)"
                                border_color = "#f97316"
                            else:
                                zone_type = f"Range Support Long{apex_str}"
                                signal = "RANGE_BUY"
                                box_color = "rgba(6, 182, 212, 0.20)"
                                border_color = "#06b6d4"
                        else:
                            if trend_state == -1:
                                zone_type = f"Pro Continuation Short{apex_str}"
                                signal = "SELL"
                                box_color = "rgba(239, 68, 68, 0.20)"
                                border_color = "#ef4444"
                            elif trend_state == 1:
                                zone_type = f"Risk: Reversal Short{apex_str}"
                                signal = "REVERSAL_SELL"
                                box_color = "rgba(249, 115, 22, 0.20)"
                                border_color = "#f97316"
                            else:
                                zone_type = f"Range Resistance Short{apex_str}"
                                signal = "RANGE_SELL"
                                box_color = "rgba(6, 182, 212, 0.20)"
                                border_color = "#06b6d4"
                    else:
                        zone_type = f"Potential {'Long' if is_lower else 'Short'} Zone{apex_str}"
                        signal = "BUY" if is_lower else "SELL"
                        border_color = "#10b981" if is_lower else "#ef4444"
                        box_color = "rgba(16, 185, 129, 0.20)" if is_lower else "rgba(239, 68, 68, 0.20)"

                elif is_pullback:
                    # HQ Pullback requires trend alignment
                    if self.use_trend_filter:
                        if is_lower and trend_state == 1:
                            zone_type = "HQ Pullback Long"
                            signal = "BUY"
                            box_color = "rgba(20, 184, 166, 0.20)"
                            border_color = "#14b8a6"
                        elif not is_lower and trend_state == -1:
                            zone_type = "HQ Pullback Short"
                            signal = "SELL"
                            box_color = "rgba(217, 70, 239, 0.20)"
                            border_color = "#d946ef"
                        else:
                            draw_zone = False  # Filter out low-quality counter-trend pullbacks
                    else:
                        zone_type = f"HQ Pullback {'Long' if is_lower else 'Short'}"
                        signal = "BUY" if is_lower else "SELL"
                        border_color = "#14b8a6" if is_lower else "#d946ef"
                        box_color = "rgba(20, 184, 166, 0.20)" if is_lower else "rgba(217, 70, 239, 0.20)"

                if draw_zone:
                    zone_obj = LiquidityZone(
                        id=f"zone_bin_{j}_{'long' if is_lower else 'short'}",
                        bin_index=j,
                        lower=b_lower,
                        upper=b_upper,
                        mid=mid,
                        volume=round(voll, 2),
                        volume_pct=vol_pct,
                        is_lower=is_lower,
                        is_major=is_major,
                        is_apex=is_apex,
                        is_pullback=is_pullback,
                        zone_type=zone_type,
                        signal=signal,
                        sl_price=sl_price,
                        start_bar=start_bar,
                        box_color=box_color,
                        border_color=border_color,
                    )

                    if is_major:
                        major_zones.append(zone_obj)
                        if is_apex:
                            apex_zone = zone_obj
                    elif is_pullback:
                        pullback_zones.append(zone_obj)

        all_active_zones = major_zones + pullback_zones

        # 8. Identify Nearest Support (Below Price) & Nearest Resistance (Above Price)
        supports = [z for z in all_active_zones if z.upper <= current_close or z.mid < current_close]
        resistances = [z for z in all_active_zones if z.lower >= current_close or z.mid > current_close]

        nearest_support: Optional[LiquidityZone] = None
        nearest_resistance: Optional[LiquidityZone] = None
        dist_sup: Optional[float] = None
        dist_res: Optional[float] = None

        if supports:
            supports.sort(key=lambda z: abs(current_close - z.upper))
            nearest_support = supports[0]
            dist_sup = round(abs(current_close - nearest_support.upper), 2)

        if resistances:
            resistances.sort(key=lambda z: abs(z.lower - current_close))
            nearest_resistance = resistances[0]
            dist_res = round(abs(nearest_resistance.lower - current_close), 2)

        return LiquidityProfileResult(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol="XAU/USD",
            current_price=round(current_close, 2),
            lookback=lb,
            bins_count=self.bins,
            trend_state=trend_state,
            trend_label=trend_label,
            fast_ma=round(fast_ma, 2),
            slow_ma=round(slow_ma, 2),
            atr=round(atr_series[-1] if atr_series else 1.0, 2),
            top_boundary=round(top, 2),
            bot_boundary=round(bot, 2),
            poc_price=poc_price,
            poc_volume=round(max_bin_vol, 2),
            apex_zone=apex_zone,
            major_zones=major_zones,
            pullback_zones=pullback_zones,
            all_active_zones=all_active_zones,
            heatmap_bins=heatmap_bins,
            nearest_support=nearest_support,
            nearest_resistance=nearest_resistance,
            distance_to_nearest_support=dist_sup,
            distance_to_nearest_resistance=dist_res,
        )


# ---------------------------------------------------------------------------
# Real-Time Zone Radar & Alert Engine
# ---------------------------------------------------------------------------

class ZoneProximityState:
    OUTSIDE = "OUTSIDE"
    NEARING = "NEARING"
    INSIDE = "INSIDE"


@dataclass
class ZoneAlertEvent:
    event_type: str        # "NEARING" or "ENTERED"
    zone: LiquidityZone
    current_price: float
    distance: float
    timestamp: str
    message: str


class ZoneRadar:
    """
    Monitors live price ticks against active liquidity zones.
    Detects when price is NEARING (within buffer USD) or INSIDE a zone.
    Maintains alert cooldown and state transitions to prevent spamming.
    """

    def __init__(self, proximity_buffer_usd: float = DEFAULT_PROXIMITY_BUFFER_USD, cooldown_seconds: float = 900.0):
        self.proximity_buffer_usd = proximity_buffer_usd
        self.cooldown_seconds = cooldown_seconds
        # Mapping: zone_id -> current state ("OUTSIDE", "NEARING", "INSIDE")
        self._zone_states: Dict[str, str] = {}
        # Mapping: (zone_id, event_type) -> last_alert_timestamp
        self._last_alert_times: Dict[Tuple[str, str], float] = {}

    def evaluate_price(
        self,
        current_price: float,
        profile: LiquidityProfileResult
    ) -> List[ZoneAlertEvent]:
        """
        Evaluates the current spot price against all active zones.
        Returns a list of ZoneAlertEvents triggered on this tick.
        """
        now = time.time()
        triggered_events: List[ZoneAlertEvent] = []

        for zone in profile.all_active_zones:
            zone_id = zone.id
            prev_state = self._zone_states.get(zone_id, ZoneProximityState.OUTSIDE)

            # Determine distance to zone boundaries
            if zone.lower <= current_price <= zone.upper:
                current_state = ZoneProximityState.INSIDE
                distance = 0.0
            elif current_price < zone.lower:
                distance = round(zone.lower - current_price, 2)
                current_state = ZoneProximityState.NEARING if distance <= self.proximity_buffer_usd else ZoneProximityState.OUTSIDE
            else:  # current_price > zone.upper
                distance = round(current_price - zone.upper, 2)
                current_state = ZoneProximityState.NEARING if distance <= self.proximity_buffer_usd else ZoneProximityState.OUTSIDE

            # Case 1: Entered Zone
            if current_state == ZoneProximityState.INSIDE and prev_state != ZoneProximityState.INSIDE:
                last_alert = self._last_alert_times.get((zone_id, "ENTERED"), 0.0)
                if now - last_alert >= self.cooldown_seconds:
                    self._last_alert_times[(zone_id, "ENTERED")] = now
                    triggered_events.append(ZoneAlertEvent(
                        event_type="ENTERED",
                        zone=zone,
                        current_price=current_price,
                        distance=0.0,
                        timestamp=datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
                        message=f"Spot price ${current_price:.2f} has reached/entered {zone.zone_type} (${zone.lower:.2f} - ${zone.upper:.2f})",
                    ))

            # Case 2: Nearing Zone
            elif current_state == ZoneProximityState.NEARING and prev_state == ZoneProximityState.OUTSIDE:
                last_alert = self._last_alert_times.get((zone_id, "NEARING"), 0.0)
                if now - last_alert >= self.cooldown_seconds:
                    self._last_alert_times[(zone_id, "NEARING")] = now
                    triggered_events.append(ZoneAlertEvent(
                        event_type="NEARING",
                        zone=zone,
                        current_price=current_price,
                        distance=distance,
                        timestamp=datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
                        message=f"Spot price ${current_price:.2f} is nearing {zone.zone_type} (only ${distance:.2f} away from ${zone.lower:.2f} - ${zone.upper:.2f})",
                    ))

            # Update recorded state
            self._zone_states[zone_id] = current_state

        return triggered_events
