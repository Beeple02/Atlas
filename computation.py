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
