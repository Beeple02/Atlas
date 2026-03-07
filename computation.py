"""
Atlas Computation Engine
Computes derived metrics from accumulated history data.
Called after every ingestion cycle.
"""

import logging
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

import database as db

logger = logging.getLogger(__name__)


def _safe_div(a: float, b: float) -> Optional[float]:
    return a / b if b and b != 0 else None


def _round(val: Optional[float], decimals: int = 4) -> Optional[float]:
    return round(val, decimals) if val is not None else None


# ── VWAP ─────────────────────────────────────────────────────────────────────

def _compute_vwap(records: list[dict]) -> Optional[float]:
    """Volume-weighted average price from a list of {price, volume} dicts."""
    total_value = 0.0
    total_volume = 0
    for r in records:
        price = r.get("price")
        volume = r.get("volume") or 0
        if price is not None and volume > 0:
            total_value += price * volume
            total_volume += volume
    if total_volume < 2:
        return None
    return _safe_div(total_value, total_volume)


async def compute_vwap(ticker: str) -> dict:
    records_7d = await db.get_price_history(ticker, days=7, limit=5000)
    records_24h = await db.get_price_history(ticker, days=1, limit=5000)
    return {
        "vwap_7d": _round(_compute_vwap(records_7d)),
        "vwap_24h": _round(_compute_vwap(records_24h)),
    }


# ── Volatility ────────────────────────────────────────────────────────────────

def _compute_volatility(records: list[dict]) -> Optional[float]:
    """
    Annualized price volatility (std dev of returns).
    Requires at least 5 data points.
    """
    if len(records) < 5:
        return None
    prices = [r["price"] for r in records if r.get("price") is not None]
    if len(prices) < 5:
        return None
    # Compute log returns
    returns = []
    for i in range(1, len(prices)):
        if prices[i - 1] > 0:
            returns.append(math.log(prices[i] / prices[i - 1]))
    if len(returns) < 4:
        return None
    mean = sum(returns) / len(returns)
    variance = sum((r - mean) ** 2 for r in returns) / (len(returns) - 1)
    std_dev = math.sqrt(variance)
    # Annualize: assume ~252 trading periods per year (adjust if needed)
    annualized = std_dev * math.sqrt(252) * 100  # as percentage
    return round(annualized, 4)


async def compute_volatility(ticker: str) -> dict:
    records = await db.get_price_history(ticker, days=7, limit=5000)
    # Sort ascending for return computation
    records_sorted = sorted(records, key=lambda r: r.get("timestamp", ""))
    return {"volatility_7d": _compute_volatility(records_sorted)}


# ── Spread & orderbook metrics ────────────────────────────────────────────────

def _compute_orderbook_metrics(orderbook: dict) -> dict:
    """
    From a live orderbook snapshot, compute:
    - spread, spread_pct
    - bid_depth, ask_depth
    - orderbook_imbalance (-1.0 to +1.0)
    """
    best_bid = orderbook.get("best_bid")
    best_ask = orderbook.get("best_ask")
    mid = orderbook.get("mid")
    bids = orderbook.get("bids", [])
    asks = orderbook.get("asks", [])

    spread = None
    spread_pct = None
    if best_bid is not None and best_ask is not None:
        spread = round(best_ask - best_bid, 4)
        if mid and mid > 0:
            spread_pct = round((spread / mid) * 100, 4)

    bid_depth = sum(b.get("quantity", 0) for b in bids)
    ask_depth = sum(a.get("quantity", 0) for a in asks)

    total_depth = bid_depth + ask_depth
    imbalance = None
    if total_depth > 0:
        imbalance = round((bid_depth - ask_depth) / total_depth, 4)

    return {
        "spread": spread,
        "spread_pct": spread_pct,
        "bid_depth": bid_depth,
        "ask_depth": ask_depth,
        "orderbook_imbalance": imbalance,
    }


# ── Liquidity score ───────────────────────────────────────────────────────────

def _compute_liquidity_score(
    spread_pct: Optional[float],
    bid_depth: float,
    ask_depth: float,
    trade_count_7d: int,
) -> Optional[float]:
    """
    Composite liquidity score 0–100.
    Components:
    - Spread tightness (40 pts): 0% spread = 40, 10%+ spread = 0
    - Depth (40 pts): depth relative to 1000 units as reference
    - Trade frequency (20 pts): 50+ trades/7d = 20
    """
    if spread_pct is None and bid_depth == 0 and ask_depth == 0:
        return None

    # Spread score (40 pts max)
    if spread_pct is not None:
        spread_score = max(0.0, 40.0 * (1 - min(spread_pct / 10.0, 1.0)))
    else:
        spread_score = 0.0

    # Depth score (40 pts max)
    total_depth = bid_depth + ask_depth
    depth_score = min(40.0, (total_depth / 1000.0) * 40.0)

    # Frequency score (20 pts max)
    freq_score = min(20.0, (trade_count_7d / 50.0) * 20.0)

    return round(spread_score + depth_score + freq_score, 2)


# ── Master computation runner ─────────────────────────────────────────────────

async def compute_all_metrics(ticker: str) -> None:
    """Compute and persist all derived metrics for a single ticker."""
    try:
        orderbook = await db.get_orderbook(ticker)
        vwap = await compute_vwap(ticker)
        vol = await compute_volatility(ticker)

        ob_metrics = {}
        if orderbook:
            ob_metrics = _compute_orderbook_metrics(orderbook)

        records_7d = await db.get_price_history(ticker, days=7, limit=5000)
        trade_count_7d = len(records_7d)

        liquidity = _compute_liquidity_score(
            ob_metrics.get("spread_pct"),
            ob_metrics.get("bid_depth", 0),
            ob_metrics.get("ask_depth", 0),
            trade_count_7d,
        )

        metrics = {
            **vwap,
            **vol,
            **ob_metrics,
            "liquidity_score": liquidity,
        }

        await db.upsert_derived(ticker, metrics)
        logger.debug("Computed metrics for %s: %s", ticker, metrics)

    except Exception as e:
        logger.error("Failed to compute metrics for %s: %s", ticker, e)


async def compute_all_tickers() -> None:
    """Run compute_all_metrics for every tracked ticker."""
    tickers = await db.get_all_tickers()
    for ticker in tickers:
        await compute_all_metrics(ticker)
    logger.info("Derived metrics recomputed for %d tickers", len(tickers))


# ── Technical indicators (for /analytics endpoints) ──────────────────────────

def _sma(prices: list[float], n: int) -> Optional[float]:
    if len(prices) < n:
        return None
    return _round(sum(prices[-n:]) / n)


def _ema(prices: list[float], n: int) -> Optional[float]:
    if len(prices) < n:
        return None
    k = 2 / (n + 1)
    ema = sum(prices[:n]) / n
    for p in prices[n:]:
        ema = p * k + ema * (1 - k)
    return _round(ema)


def _ema_series(prices: list[float], n: int) -> list[float]:
    if len(prices) < n:
        return []
    k = 2 / (n + 1)
    ema = sum(prices[:n]) / n
    result = [ema]
    for p in prices[n:]:
        ema = p * k + ema * (1 - k)
        result.append(ema)
    return result


def _macd(prices: list[float]) -> tuple[Optional[float], Optional[float], Optional[float]]:
    """Returns (macd_line, signal, histogram). Uses 12/26/9 EMA."""
    if len(prices) < 35:
        return None, None, None
    ema12 = _ema_series(prices, 12)
    ema26 = _ema_series(prices, 26)
    # Align: ema26 is shorter, trim ema12 to match
    diff = len(ema12) - len(ema26)
    ema12_aligned = ema12[diff:]
    macd_line_series = [a - b for a, b in zip(ema12_aligned, ema26)]
    if len(macd_line_series) < 9:
        return None, None, None
    signal_series = _ema_series(macd_line_series, 9)
    if not signal_series:
        return None, None, None
    macd_val = macd_line_series[-1]
    signal_val = signal_series[-1]
    return _round(macd_val), _round(signal_val), _round(macd_val - signal_val)


def _rsi(prices: list[float], n: int = 14) -> Optional[float]:
    if len(prices) < n + 1:
        return None
    deltas = [prices[i] - prices[i-1] for i in range(1, len(prices))]
    gains = [d if d > 0 else 0 for d in deltas[-n:]]
    losses = [-d if d < 0 else 0 for d in deltas[-n:]]
    avg_gain = sum(gains) / n
    avg_loss = sum(losses) / n
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return _round(100 - (100 / (1 + rs)))


def _atr(candles: list[dict], n: int = 14) -> Optional[float]:
    if len(candles) < n + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h = candles[i].get("high") or 0
        l = candles[i].get("low") or 0
        pc = candles[i-1].get("close") or 0
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return _round(sum(trs[-n:]) / n)


def _bollinger(prices: list[float], n: int = 20, k: float = 2.0) -> tuple[Optional[float], Optional[float]]:
    if len(prices) < n:
        return None, None
    window = prices[-n:]
    mean = sum(window) / n
    std = math.sqrt(sum((p - mean) ** 2 for p in window) / n)
    return _round(mean + k * std), _round(mean - k * std)


def _sharpe(prices: list[float], risk_free: float = 0.0) -> Optional[float]:
    if len(prices) < 10:
        return None
    returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    if len(returns) < 5:
        return None
    mean_r = sum(returns) / len(returns)
    std_r = math.sqrt(sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1))
    if std_r == 0:
        return None
    return _round((mean_r - risk_free / 252) / std_r * math.sqrt(252))


def _max_drawdown(prices: list[float]) -> Optional[float]:
    if len(prices) < 2:
        return None
    peak = prices[0]
    max_dd = 0.0
    for p in prices:
        if p > peak:
            peak = p
        dd = (peak - p) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return _round(max_dd * 100)


def _downside_vol(prices: list[float]) -> Optional[float]:
    if len(prices) < 5:
        return None
    returns = [math.log(prices[i] / prices[i-1]) for i in range(1, len(prices)) if prices[i-1] > 0]
    neg = [r for r in returns if r < 0]
    if len(neg) < 3:
        return None
    mean_neg = sum(neg) / len(neg)
    std = math.sqrt(sum((r - mean_neg) ** 2 for r in neg) / (len(neg) - 1))
    return _round(std * math.sqrt(252) * 100)


def _mean_reversion_score(prices: list[float], n: int = 20) -> Optional[float]:
    """Z-score of current price vs n-day mean."""
    if len(prices) < n:
        return None
    window = prices[-n:]
    mean = sum(window) / n
    std = math.sqrt(sum((p - mean) ** 2 for p in window) / n)
    if std == 0:
        return None
    return _round((prices[-1] - mean) / std)


def _vol_spike(prices: list[float]) -> Optional[float]:
    """5-day vol / 20-day vol ratio."""
    if len(prices) < 21:
        return None
    def _vol(p):
        if len(p) < 2:
            return None
        rets = [math.log(p[i]/p[i-1]) for i in range(1, len(p)) if p[i-1] > 0]
        if len(rets) < 2:
            return None
        mean = sum(rets) / len(rets)
        return math.sqrt(sum((r - mean)**2 for r in rets) / (len(rets) - 1))
    v5  = _vol(prices[-6:])
    v20 = _vol(prices[-21:])
    if v5 is None or v20 is None or v20 == 0:
        return None
    return _round(v5 / v20)


def compute_ohlcv_analytics(candles: list[dict]) -> dict:
    """
    Compute all technical indicators from OHLCV candles.
    Returns a flat dict of indicator values.
    """
    if not candles:
        return {}

    # Sort ascending by date
    candles = sorted(candles, key=lambda c: c.get("date", ""))
    closes  = [c["close"]  for c in candles if c.get("close")  is not None]
    volumes = [c["volume"] for c in candles if c.get("volume") is not None]

    if not closes:
        return {}

    vwap_num = sum((c.get("close") or 0) * (c.get("volume") or 0) for c in candles)
    vwap_den = sum(c.get("volume") or 0 for c in candles)
    vwap = _round(vwap_num / vwap_den) if vwap_den > 0 else None

    vol_ma20 = _round(sum(volumes[-20:]) / min(len(volumes), 20)) if volumes else None
    avg_vol  = _round(sum(volumes) / len(volumes)) if volumes else None

    bb_upper, bb_lower = _bollinger(closes)
    macd_line, macd_signal, macd_hist = _macd(closes)

    ann_vol = None
    if len(closes) >= 5:
        rets = [math.log(closes[i]/closes[i-1]) for i in range(1, len(closes)) if closes[i-1] > 0]
        if len(rets) >= 4:
            mean_r = sum(rets) / len(rets)
            std_r = math.sqrt(sum((r - mean_r)**2 for r in rets) / (len(rets) - 1))
            ann_vol = _round(std_r * math.sqrt(252) * 100)

    price_chg = None
    if len(closes) >= 2:
        price_chg = _round((closes[-1] - closes[0]) / closes[0] * 100) if closes[0] > 0 else None

    return {
        "sma20":             _sma(closes, 20),
        "sma50":             _sma(closes, 50),
        "sma100":            _sma(closes, 100),
        "vwap":              vwap,
        "macd_line":         macd_line,
        "macd_signal":       macd_signal,
        "macd_hist":         macd_hist,
        "rsi14":             _rsi(closes),
        "atr14":             _atr(candles),
        "bb_upper":          bb_upper,
        "bb_lower":          bb_lower,
        "vol_ma20":          vol_ma20,
        "volatility_ann_pct": ann_vol,
        "sharpe":            _sharpe(closes),
        "max_drawdown_pct":  _max_drawdown(closes),
        "price_chg_pct":     price_chg,
        "avg_volume":        avg_vol,
        "high_period":       _round(max((c.get("high") or 0) for c in candles)),
        "low_period":        _round(min((c.get("low")  or float("inf")) for c in candles)),
        "mean_reversion_score": _mean_reversion_score(closes),
        "downside_vol":      _downside_vol(closes),
        "vol_spike":         _vol_spike(closes),
        "near_high":         _round(
            (closes[-1] - min(c.get("low") or closes[-1] for c in candles)) /
            (max(c.get("high") or closes[-1] for c in candles) - min(c.get("low") or closes[-1] for c in candles))
            if (max(c.get("high") or closes[-1] for c in candles) - min(c.get("low") or closes[-1] for c in candles)) > 0
            else None
        ),
    }


def compute_holder_intel(shareholders: list[dict], total_shares: int) -> dict:
    """Compute ownership concentration metrics."""
    if not shareholders or not total_shares:
        return {}

    holders = sorted(shareholders, key=lambda h: h.get("quantity") or 0, reverse=True)
    quantities = [h.get("quantity") or 0 for h in holders]
    total = total_shares

    pcts = [q / total for q in quantities if total > 0]

    # HHI (0–10000)
    hhi = _round(sum((p * 100) ** 2 for p in pcts))

    # Gini coefficient
    n = len(pcts)
    gini = None
    if n > 1:
        sorted_pcts = sorted(pcts)
        gini = _round((2 * sum((i+1) * p for i, p in enumerate(sorted_pcts)) / (n * sum(sorted_pcts))) - (n+1)/n)

    top1_pct  = _round(pcts[0] * 100) if pcts else None
    top5_pct  = _round(sum(pcts[:5]) * 100) if pcts else None
    whale_count = sum(1 for p in pcts if p >= 0.10)

    # Histogram buckets
    buckets = [("0–1%", 0, 0.01), ("1–5%", 0.01, 0.05),
               ("5–10%", 0.05, 0.10), ("10–25%", 0.10, 0.25), ("25%+", 0.25, 1.01)]
    histogram = [
        {"bucket": label, "count": sum(1 for p in pcts if lo <= p < hi)}
        for label, lo, hi in buckets
    ]

    enriched_holders = [
        {**h, "pct": _round((h.get("quantity") or 0) / total * 100)}
        for h in holders
    ]

    return {
        "stats": {
            "num_holders": len(holders),
            "hhi": hhi,
            "top1_pct": top1_pct,
            "top5_pct": top5_pct,
            "whale_count": whale_count,
            "gini": gini,
        },
        "holders": enriched_holders,
        "histogram": histogram,
    }
