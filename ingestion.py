"""
Atlas Ingestion Layer
Polls NER API endpoints on schedule and processes webhook events.
Never blocks the FastAPI serving layer — all async.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import database as db
import computation
from config import settings

logger = logging.getLogger(__name__)

# Shared aiohttp session (created at startup, closed at shutdown)
_session: Optional[aiohttp.ClientSession] = None


def get_session() -> aiohttp.ClientSession:
    if _session is None or _session.closed:
        raise RuntimeError("HTTP session not initialized. Call init_session() first.")
    return _session


async def init_session() -> None:
    global _session
    _session = aiohttp.ClientSession(
        headers={
            "Content-Type": "application/json",
            "X-API-Key": settings.ner_api_key,
        },
        timeout=aiohttp.ClientTimeout(total=settings.ner_request_timeout),
    )
    logger.info("aiohttp session initialized")


async def close_session() -> None:
    global _session
    if _session and not _session.closed:
        await _session.close()
        logger.info("aiohttp session closed")


# ── NER API helpers ───────────────────────────────────────────────────────────

async def _get(path: str, params: dict = None) -> Optional[dict | list]:
    """Make a GET request to NER API. Returns parsed JSON or None on error."""
    url = f"{settings.ner_base_url}{path}"
    try:
        async with get_session().get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status == 429:
                logger.warning("NER rate limit hit on %s — backing off 60s", path)
                await db.set_meta("ner_rate_limited_at", datetime.now(timezone.utc).isoformat())
                await asyncio.sleep(60)
                return None
            elif resp.status == 404:
                logger.debug("NER 404 on %s", path)
                return None
            else:
                logger.warning("NER API %s returned %d", path, resp.status)
                return None
    except asyncio.TimeoutError:
        logger.error("Timeout hitting NER API %s", path)
        return None
    except aiohttp.ClientError as e:
        logger.error("HTTP error on NER API %s: %s", path, e)
        return None


async def _check_ner_health() -> bool:
    """Quick health check against NER API."""
    try:
        url = f"{settings.ner_base_url}/health"
        async with get_session().get(url) as resp:
            return resp.status == 200
    except Exception:
        return False


# ── Poll: all securities ──────────────────────────────────────────────────────

async def poll_securities() -> None:
    logger.info("Polling /securities...")
    data = await _get("/securities")
    if data is None:
        await db.set_meta("ner_reachable", "false")
        return
    await db.set_meta("ner_reachable", "true")
    await db.upsert_securities(data)
    await db.set_meta("last_poll_securities", datetime.now(timezone.utc).isoformat())
    logger.info("Securities updated: %d tickers", len(data))
    # Recompute derived for all (market_price may have changed)
    await computation.compute_all_tickers()


# ── Poll: orderbook (all tickers in one call) ─────────────────────────────────

async def poll_orderbook() -> None:
    logger.info("Polling /orderbook...")
    data = await _get("/orderbook")
    if data is None:
        return
    if isinstance(data, list):
        for entry in data:
            ticker = entry.get("ticker")
            if ticker:
                await db.upsert_orderbook(ticker, entry)
    elif isinstance(data, dict):
        ticker = data.get("ticker")
        if ticker:
            await db.upsert_orderbook(ticker, data)
    await db.set_meta("last_poll_orderbook", datetime.now(timezone.utc).isoformat())
    logger.info("Orderbook updated")
    await computation.compute_all_tickers()


# ── Poll: price history (staggered per ticker) ────────────────────────────────

async def poll_price_history() -> None:
    tickers = await db.get_all_tickers()
    logger.info("Polling price history for %d tickers (staggered)...", len(tickers))
    for i, ticker in enumerate(tickers):
        if i > 0:
            await asyncio.sleep(settings.stagger_delay)
        data = await _get(f"/analytics/price_history/{ticker}",
                          params={"days": settings.price_history_days})
        if data:
            await db.insert_price_history(ticker, data, source="ner_api")
    await db.set_meta("last_poll_price_history", datetime.now(timezone.utc).isoformat())
    logger.info("Price history updated for %d tickers", len(tickers))
    await computation.compute_all_tickers()


# ── Poll: OHLCV (staggered per ticker) ───────────────────────────────────────

async def poll_ohlcv() -> None:
    tickers = await db.get_all_tickers()
    logger.info("Polling OHLCV for %d tickers (staggered)...", len(tickers))
    for i, ticker in enumerate(tickers):
        if i > 0:
            await asyncio.sleep(settings.stagger_delay)
        data = await _get(f"/analytics/ohlcv/{ticker}", params={"days": 90})
        if data and "candles" in data:
            await db.upsert_ohlcv(ticker, data["candles"])
    await db.set_meta("last_poll_ohlcv", datetime.now(timezone.utc).isoformat())
    logger.info("OHLCV updated")


# ── Poll: shareholders (staggered per ticker) ─────────────────────────────────

async def poll_shareholders() -> None:
    tickers = await db.get_all_tickers()
    logger.info("Polling shareholders for %d tickers (staggered)...", len(tickers))
    for i, ticker in enumerate(tickers):
        if i > 0:
            await asyncio.sleep(settings.stagger_delay)
        data = await _get("/shareholders", params={"ticker": ticker})
        if data:
            await db.upsert_shareholders(ticker, data)
    await db.set_meta("last_poll_shareholders", datetime.now(timezone.utc).isoformat())
    logger.info("Shareholders updated")


# ── Poll: financial stats (staggered per ticker) ──────────────────────────────

async def poll_stats() -> None:
    tickers = await db.get_all_tickers()
    logger.info("Polling stats for %d tickers (staggered)...", len(tickers))
    for i, ticker in enumerate(tickers):
        if i > 0:
            await asyncio.sleep(settings.stagger_delay)
        data = await _get(f"/securities/{ticker}/stats")
        if data:
            await db.upsert_stats(ticker, data)
    await db.set_meta("last_poll_stats", datetime.now(timezone.utc).isoformat())
    logger.info("Stats updated")


# ── Webhook event processor ───────────────────────────────────────────────────

async def process_webhook_event(payload: dict) -> None:
    """
    Handle a NER market_update webhook event.
    Payload shape:
    {
        "event": "market_update",
        "ticker": "RTG",
        "market_price": 14.5,
        "frozen": false,
        "orderbook": { "bids": [...], "asks": [...], "best_bid": ..., "best_ask": ..., "mid": ... },
        "updated_at": "..."
    }
    """
    event = payload.get("event")
    ticker = payload.get("ticker")

    if not ticker:
        logger.warning("Webhook payload missing ticker: %s", payload)
        return

    if event == "market_update":
        # Update market price
        price = payload.get("market_price")
        frozen = payload.get("frozen", False)
        if price is not None:
            await db.update_market_price(ticker, price, frozen)
            logger.debug("Webhook: updated price for %s → %.4f", ticker, price)

        # Update orderbook
        orderbook = payload.get("orderbook")
        if orderbook:
            orderbook["ticker"] = ticker
            await db.upsert_orderbook(ticker, orderbook)
            logger.debug("Webhook: updated orderbook for %s", ticker)

        # Insert into price history so it feeds VWAP/volatility
        if price is not None:
            await db.insert_price_history(
                ticker,
                [{"price": price, "volume": 0, "timestamp": payload.get("updated_at")}],
                source="webhook"
            )

        # Immediately recompute derived metrics for this ticker
        await computation.compute_all_metrics(ticker)

    else:
        logger.warning("Unknown webhook event type: %s", event)


# ── Scheduler setup ───────────────────────────────────────────────────────────

def create_scheduler() -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")

    scheduler.add_job(
        poll_securities,
        "interval",
        seconds=settings.poll_securities_interval,
        id="poll_securities",
        max_instances=1,
        misfire_grace_time=30,
    )
    scheduler.add_job(
        poll_orderbook,
        "interval",
        seconds=settings.poll_orderbook_interval,
        id="poll_orderbook",
        max_instances=1,
        misfire_grace_time=30,
    )
    scheduler.add_job(
        poll_price_history,
        "interval",
        seconds=settings.poll_price_history_interval,
        id="poll_price_history",
        max_instances=1,
        misfire_grace_time=60,
    )
    scheduler.add_job(
        poll_ohlcv,
        "interval",
        seconds=settings.poll_ohlcv_interval,
        id="poll_ohlcv",
        max_instances=1,
        misfire_grace_time=60,
    )
    scheduler.add_job(
        poll_shareholders,
        "interval",
        seconds=settings.poll_shareholders_interval,
        id="poll_shareholders",
        max_instances=1,
        misfire_grace_time=60,
    )
    scheduler.add_job(
        poll_stats,
        "interval",
        seconds=settings.poll_stats_interval,
        id="poll_stats",
        max_instances=1,
        misfire_grace_time=60,
    )

    return scheduler


async def run_initial_sync() -> None:
    """
    Run all polls once at startup so Atlas has data before serving requests.
    Securities first (needed to know which tickers exist), then everything else.
    """
    logger.info("Running initial full sync...")
    await poll_securities()
    await asyncio.gather(
        poll_orderbook(),
        poll_price_history(),
        poll_ohlcv(),
        poll_shareholders(),
        poll_stats(),
    )
    await db.set_meta("atlas_initialized", "true")
    logger.info("Initial sync complete")
