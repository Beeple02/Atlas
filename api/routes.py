"""
Atlas API Routes
All REST endpoints served to internal Bloomberg Labs tools.
"""

import hashlib
import hmac
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse

import database as db
import ingestion
from auth import require_auth
from config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

_startup_time = datetime.now(timezone.utc)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_initialized() -> bool:
    """Synchronous check is not possible here — handled at route level via meta."""
    return True  # Checked async in routes that need it


async def _assert_initialized():
    initialized = await db.get_meta("atlas_initialized")
    if initialized != "true":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"detail": "Atlas is initializing. Retry in a few seconds.", "code": "ATLAS_INITIALIZING"}
        )


async def _assert_ticker_exists(ticker: str):
    sec = await db.get_security(ticker)
    if not sec:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"detail": f"Ticker '{ticker}' not found in Atlas.", "code": "TICKER_NOT_FOUND"}
        )
    return sec


# ── Health & Status ───────────────────────────────────────────────────────────

@router.get("/health")
async def health():
    return {"status": "ok", "service": "Atlas", "version": "1.0.0"}


@router.get("/status")
async def atlas_status():
    uptime = (datetime.now(timezone.utc) - _startup_time).total_seconds()
    last_polls = {}
    for key in ["securities", "orderbook", "price_history", "ohlcv", "shareholders", "stats"]:
        last_polls[key] = await db.get_meta(f"last_poll_{key}")

    ner_reachable_str = await db.get_meta("ner_reachable")
    initialized = await db.get_meta("atlas_initialized")
    db_stats = await db.get_db_stats()

    return {
        "status": "ok" if initialized == "true" else "initializing",
        "uptime_seconds": round(uptime),
        "initialized": initialized == "true",
        "last_poll": last_polls,
        "ner_api_reachable": ner_reachable_str != "false",
        "db_stats": db_stats,
        "generated_at": _utcnow(),
    }


# ── Securities ────────────────────────────────────────────────────────────────

@router.get("/securities")
async def list_securities(
    include_derived: bool = Query(default=True),
    frozen: Optional[bool] = Query(default=None),
    _auth=Depends(require_auth),
):
    await _assert_initialized()
    securities = await db.get_all_securities()

    if frozen is not None:
        securities = [s for s in securities if bool(s["frozen"]) == frozen]

    if include_derived:
        derived_map = {d["ticker"]: d for d in await db.get_all_derived()}
        for sec in securities:
            d = derived_map.get(sec["ticker"], {})
            sec["derived"] = {
                "vwap_24h": d.get("vwap_24h"),
                "vwap_7d": d.get("vwap_7d"),
                "volatility_7d": d.get("volatility_7d"),
                "spread": d.get("spread"),
                "spread_pct": d.get("spread_pct"),
                "orderbook_imbalance": d.get("orderbook_imbalance"),
                "liquidity_score": d.get("liquidity_score"),
            }

    return securities


@router.get("/securities/{ticker}")
async def get_security(
    ticker: str,
    _auth=Depends(require_auth),
):
    await _assert_initialized()
    ticker = ticker.upper()
    sec = await _assert_ticker_exists(ticker)
    orderbook = await db.get_orderbook(ticker)
    derived = await db.get_derived(ticker)
    stats = await db.get_stats(ticker)

    ob_summary = None
    if orderbook:
        ob_summary = {
            "best_bid": orderbook.get("best_bid"),
            "best_ask": orderbook.get("best_ask"),
            "mid": orderbook.get("mid"),
            "bid_depth": derived.get("bid_depth") if derived else None,
            "ask_depth": derived.get("ask_depth") if derived else None,
            "imbalance": derived.get("orderbook_imbalance") if derived else None,
            "captured_at": orderbook.get("captured_at"),
        }

    return {
        **sec,
        "orderbook_summary": ob_summary,
        "derived": {
            "vwap_24h": derived.get("vwap_24h") if derived else None,
            "vwap_7d": derived.get("vwap_7d") if derived else None,
            "volatility_7d": derived.get("volatility_7d") if derived else None,
            "spread": derived.get("spread") if derived else None,
            "spread_pct": derived.get("spread_pct") if derived else None,
            "liquidity_score": derived.get("liquidity_score") if derived else None,
        } if derived else None,
        "stats": stats,
    }


# ── Price ─────────────────────────────────────────────────────────────────────

@router.get("/price/{ticker}")
async def get_price(ticker: str, _auth=Depends(require_auth)):
    await _assert_initialized()
    ticker = ticker.upper()
    sec = await _assert_ticker_exists(ticker)
    return {
        "ticker": ticker,
        "market_price": sec["market_price"],
        "frozen": bool(sec["frozen"]),
        "atlas_updated_at": sec["updated_at"],
    }


# ── Orderbook ─────────────────────────────────────────────────────────────────

async def _enrich_orderbook(ob: dict, ticker: str) -> dict:
    """Add derived fields to a raw orderbook dict."""
    derived = await db.get_derived(ticker)
    ob["bid_depth"] = derived.get("bid_depth") if derived else None
    ob["ask_depth"] = derived.get("ask_depth") if derived else None
    ob["imbalance"] = derived.get("orderbook_imbalance") if derived else None
    ob["spread"] = derived.get("spread") if derived else None
    ob["spread_pct"] = derived.get("spread_pct") if derived else None
    ob["atlas_captured_at"] = ob.pop("captured_at", None)
    return ob


@router.get("/orderbook/{ticker}")
async def get_orderbook(ticker: str, _auth=Depends(require_auth)):
    await _assert_initialized()
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    ob = await db.get_orderbook(ticker)
    if not ob:
        raise HTTPException(status_code=404, detail={"detail": "No orderbook data for this ticker.", "code": "TICKER_NOT_FOUND"})
    return await _enrich_orderbook(ob, ticker)


@router.get("/orderbook")
async def get_all_orderbooks(_auth=Depends(require_auth)):
    await _assert_initialized()
    books = await db.get_all_orderbooks()
    result = []
    for ob in books:
        enriched = await _enrich_orderbook(ob, ob["ticker"])
        result.append(enriched)
    return result


# ── History ───────────────────────────────────────────────────────────────────

@router.get("/history/{ticker}")
async def get_history(
    ticker: str,
    days: int = Query(default=30, ge=1, le=365),
    limit: int = Query(default=500, ge=1, le=5000),
    from_dt: Optional[str] = Query(default=None, alias="from"),
    to_dt: Optional[str] = Query(default=None, alias="to"),
    _auth=Depends(require_auth),
):
    await _assert_initialized()
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    records = await db.get_price_history(ticker, days=days, limit=limit,
                                          from_dt=from_dt, to_dt=to_dt)
    return {
        "ticker": ticker,
        "count": len(records),
        "data": records,
    }


# ── OHLCV ─────────────────────────────────────────────────────────────────────

@router.get("/ohlcv/{ticker}")
async def get_ohlcv(
    ticker: str,
    days: int = Query(default=30, ge=1, le=365),
    _auth=Depends(require_auth),
):
    await _assert_initialized()
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    candles = await db.get_ohlcv(ticker, days=days)
    return {
        "ticker": ticker,
        "days": days,
        "candles": candles,
    }


# ── Shareholders ──────────────────────────────────────────────────────────────

@router.get("/shareholders/{ticker}")
async def get_shareholders(ticker: str, _auth=Depends(require_auth)):
    await _assert_initialized()
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    return await db.get_shareholders(ticker)


# ── Derived metrics ───────────────────────────────────────────────────────────

@router.get("/derived/{ticker}")
async def get_derived(ticker: str, _auth=Depends(require_auth)):
    await _assert_initialized()
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    derived = await db.get_derived(ticker)
    if not derived:
        raise HTTPException(status_code=404, detail={"detail": "No derived metrics computed yet.", "code": "TICKER_NOT_FOUND"})
    return derived


@router.get("/derived")
async def get_all_derived(_auth=Depends(require_auth)):
    await _assert_initialized()
    return await db.get_all_derived()


# ── Market summary ────────────────────────────────────────────────────────────

@router.get("/market/summary")
async def market_summary(_auth=Depends(require_auth)):
    await _assert_initialized()
    securities = await db.get_all_securities()
    derived_list = await db.get_all_derived()
    derived_map = {d["ticker"]: d for d in derived_list}

    total_market_cap = sum(s.get("market_cap") or 0 for s in securities)
    active = [s for s in securities if not s["frozen"]]
    frozen = [s for s in securities if s["frozen"]]

    # Sort by liquidity for top liquid
    by_liquidity = sorted(
        [(s["ticker"], derived_map.get(s["ticker"], {}).get("liquidity_score") or 0) for s in active],
        key=lambda x: x[1], reverse=True
    )

    return {
        "total_securities": len(securities),
        "active_securities": len(active),
        "frozen_securities": len(frozen),
        "total_market_cap": round(total_market_cap, 2),
        "most_liquid": [{"ticker": t, "liquidity_score": s} for t, s in by_liquidity[:5]],
        "tickers": [s["ticker"] for s in securities],
        "generated_at": _utcnow(),
    }


# ── Webhook receiver ──────────────────────────────────────────────────────────

@router.post("/webhook/ner")
async def ner_webhook(request: Request):
    """
    Receives push updates from NER API.
    Validates webhook secret if configured, then processes the event.
    This endpoint is NOT authenticated with X-Atlas-Key — it uses X-Webhook-Secret.
    """
    # Validate webhook secret
    if settings.webhook_secret:
        incoming_secret = request.headers.get("X-Webhook-Secret", "")
        if not hmac.compare_digest(incoming_secret, settings.webhook_secret):
            logger.warning("Webhook received with invalid secret")
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    payload = await request.json()
    await ingestion.process_webhook_event(payload)
    return {"status": "ok"}


# ── Admin: force refresh ──────────────────────────────────────────────────────

@router.post("/admin/refresh/{ticker}")
async def force_refresh_ticker(ticker: str, _auth=Depends(require_auth)):
    ticker = ticker.upper()
    await _assert_ticker_exists(ticker)
    # Queue a background refresh without blocking the response
    import asyncio
    asyncio.create_task(_refresh_ticker(ticker))
    return {"status": "refresh_queued", "ticker": ticker, "queued_at": _utcnow()}


@router.post("/admin/refresh/all")
async def force_refresh_all(_auth=Depends(require_auth)):
    import asyncio
    asyncio.create_task(ingestion.run_initial_sync())
    return {"status": "refresh_queued", "ticker": "all", "queued_at": _utcnow()}


async def _refresh_ticker(ticker: str):
    try:
        data = await ingestion._get(f"/securities/{ticker}")
        if data:
            await db.upsert_securities([data])
        ob = await ingestion._get(f"/orderbook", params={"ticker": ticker})
        if ob:
            await db.upsert_orderbook(ticker, ob)
        history = await ingestion._get(f"/analytics/price_history/{ticker}",
                                        params={"days": settings.price_history_days})
        if history:
            await db.insert_price_history(ticker, history)
        from computation import compute_all_metrics
        await compute_all_metrics(ticker)
        logger.info("Force refresh complete for %s", ticker)
    except Exception as e:
        logger.error("Force refresh failed for %s: %s", ticker, e)


# ── Admin: key management ─────────────────────────────────────────────────────

@router.get("/admin/keys")
async def list_keys(_auth=Depends(require_auth)):
    return await db.list_api_keys()


@router.post("/admin/keys")
async def create_key(
    tool_id: str = Query(..., description="Unique ID for the tool, e.g. bloomberg_terminal"),
    tool_name: str = Query(..., description="Human-readable name"),
    _auth=Depends(require_auth),
):
    from auth import create_tool_key
    try:
        key = await create_tool_key(tool_id, tool_name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "tool_id": tool_id,
        "tool_name": tool_name,
        "key": key,
        "warning": "Save this key now. It will not be shown again.",
    }


@router.delete("/admin/keys/{tool_id}")
async def revoke_key(tool_id: str, _auth=Depends(require_auth)):
    await db.deactivate_api_key(tool_id)
    return {"status": "revoked", "tool_id": tool_id}
