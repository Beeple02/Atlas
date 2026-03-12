"""
Atlas TSE Ingestion Layer
Polls The Stock Exchange (TSE) API for stocks, bonds, options, and prediction contracts.
All tickers are prefixed TSE: to avoid namespace collision with NER tickers.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp

import database as db
from config import settings

logger = logging.getLogger(__name__)

BASE = settings.tse_base_url.rstrip("/")
HEADERS = {"X-API-Key": settings.tse_api_key} if settings.tse_api_key else {}
TIMEOUT = aiohttp.ClientTimeout(total=settings.tse_request_timeout)

TSE_ENABLED = bool(settings.tse_api_key)


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def _get(path: str, params: dict = None) -> Optional[dict | list]:
    if not TSE_ENABLED:
        return None
    url = f"{BASE}{path}"
    try:
        async with aiohttp.ClientSession(headers=HEADERS, timeout=TIMEOUT) as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    return await resp.json()
                elif resp.status == 404:
                    return None
                else:
                    logger.warning("TSE %s → %d", path, resp.status)
                    return None
    except asyncio.TimeoutError:
        logger.warning("TSE timeout: %s", path)
    except Exception as e:
        logger.error("TSE request error %s: %s", path, e)
    return None


def _prefix(symbol: str) -> str:
    """Ensure TSE: prefix on ticker."""
    s = symbol.upper().strip()
    return s if s.startswith("TSE:") else f"TSE:{s}"


def _hours_to_expiry(expiration_ts: str) -> Optional[float]:
    if not expiration_ts:
        return None
    try:
        exp = datetime.fromisoformat(expiration_ts.replace("Z", "+00:00"))
        delta = exp - datetime.now(timezone.utc)
        return max(0.0, delta.total_seconds() / 3600)
    except Exception:
        return None


# ── Stocks ────────────────────────────────────────────────────────────────────

def _float(v):
    """Cast TSE string numerics to float safely."""
    try: return float(v) if v is not None else None
    except (ValueError, TypeError): return None


async def poll_tse_securities() -> None:
    """Fetch all TSE-listed stocks and upsert into securities table.
    Enriches each security with live market data from /api/v1/market/{symbol}
    since /api/v1/stocks does not reliably populate current_price.
    """
    if not TSE_ENABLED:
        return
    data = await _get("/api/v1/stocks")
    if not data:
        return

    stocks = data if isinstance(data, list) else data.get("stocks", data.get("data", []))
    securities = []
    for s in stocks:
        symbol = s.get("symbol") or s.get("ticker") or ""
        if not symbol:
            continue
        ticker = _prefix(symbol)

        # Enrich with live market data — /api/v1/stocks doesn't populate
        # current_price for tickers without recent trades, but /api/v1/market/{symbol}
        # always has lastPrice from the last known trade.
        market = await _get(f"/api/v1/market/{symbol}") or {}
        await asyncio.sleep(0.05)

        last_price = _float(market.get("lastPrice")) or _float(s.get("current_price") or s.get("price"))
        total_shares = s.get("total_shares") or s.get("shares_outstanding") or 0
        market_cap = _float(market.get("marketCap")) or _float(s.get("market_cap"))
        if market_cap is None and last_price and total_shares:
            market_cap = last_price * total_shares

        best_bid = _float(market.get("bestBid"))
        best_ask = _float(market.get("bestAsk"))

        securities.append({
            "ticker": ticker,
            "full_name": s.get("company_name") or s.get("name") or s.get("full_name"),
            "market_price": last_price,
            "total_shares": total_shares,
            "market_cap": market_cap,
            "shareholder_count": s.get("shareholder_count") or s.get("holder_count"),
            "frozen": s.get("status", "active") != "active",
            "hidden": False,
            "security_type": s.get("sector") or "stock",
        })

        # Also update orderbook cache if market data has bid/ask
        if best_bid is not None or best_ask is not None:
            mid = ((best_bid + best_ask) / 2) if best_bid and best_ask else (best_bid or best_ask)
            spread = _float(market.get("spread"))
            await db.upsert_orderbook(ticker, {
                "bids": [[best_bid, None]] if best_bid else [],
                "asks": [[best_ask, None]] if best_ask else [],
                "best_bid": best_bid,
                "best_ask": best_ask,
                "mid": mid,
                "spread": spread,
                "source": "tse",
            })

    if securities:
        await db.upsert_securities_with_source(securities, "tse")
        await db.set_meta("tse_last_securities_poll", datetime.now(timezone.utc).isoformat())
        logger.info("TSE: upserted %d securities", len(securities))


async def poll_tse_prices(ticker_symbol: str) -> None:
    """Fetch recent trades for a single TSE stock."""
    symbol = ticker_symbol.replace("TSE:", "")
    data = await _get(f"/api/v1/market/{symbol}/trades", params={"limit": 100})
    if not data:
        return

    trades = data if isinstance(data, list) else data.get("trades", data.get("data", []))
    records = []
    for t in trades:
        records.append({
            "price": t.get("price"),
            "volume": t.get("quantity") or t.get("volume"),
            "timestamp": t.get("timestamp") or t.get("created_at"),
        })

    if records:
        await db.insert_price_history(_prefix(symbol), records, source="tse")


async def poll_tse_orderbook(ticker_symbol: str) -> None:
    """Fetch orderbook for a single TSE stock."""
    symbol = ticker_symbol.replace("TSE:", "")
    data = await _get(f"/api/v1/market/{symbol}/orderbook")
    if not data:
        return

    bids = data.get("bids", [])
    asks = data.get("asks", [])
    best_bid = bids[0].get("price") if bids else None
    best_ask = asks[0].get("price") if asks else None
    mid = ((best_bid + best_ask) / 2) if best_bid and best_ask else None

    await db.upsert_orderbook(_prefix(symbol), {
        "bids": bids, "asks": asks,
        "best_bid": best_bid, "best_ask": best_ask, "mid": mid,
        "source": "tse",
    })


async def poll_tse_ohlcv(ticker_symbol: str, interval: str = "1h") -> None:
    """Fetch candles for a single TSE stock."""
    symbol = ticker_symbol.replace("TSE:", "")
    data = await _get(f"/api/v1/market/{symbol}/candles", params={"interval": interval, "limit": 500})
    if not data:
        return

    candles_raw = data if isinstance(data, list) else data.get("candles", data.get("data", []))
    candles = []
    for c in candles_raw:
        ts = c.get("timestamp") or c.get("time") or c.get("date")
        # Normalize to date string for daily, or keep ISO for intraday
        if ts and "T" in str(ts):
            date_key = str(ts)[:10] if interval == "1d" else ts
        else:
            date_key = str(ts) if ts else None

        if date_key:
            candles.append({
                "date": date_key,
                "open": c.get("open"),
                "high": c.get("high"),
                "low": c.get("low"),
                "close": c.get("close"),
                "volume": c.get("volume"),
            })

    if candles:
        # Store with source + interval
        import aiosqlite
        from config import settings as cfg
        async with aiosqlite.connect(cfg.db_path) as conn:
            await conn.executemany(
                """INSERT OR REPLACE INTO ohlcv(ticker, date, open, high, low, close, volume, source, interval)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'tse', ?)""",
                [(_prefix(symbol), c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"], interval)
                 for c in candles]
            )
            await conn.commit()
        logger.debug("TSE ohlcv %s [%s]: %d candles", symbol, interval, len(candles))


async def poll_tse_market(ticker_symbol: str) -> None:
    """Pull /api/v1/market/{symbol} and update market_price + orderbook cache."""
    symbol = ticker_symbol.replace("TSE:", "")
    market = await _get(f"/api/v1/market/{symbol}")
    if not market:
        return

    last_price = _float(market.get("lastPrice"))
    best_bid   = _float(market.get("bestBid"))
    best_ask   = _float(market.get("bestAsk"))
    spread     = _float(market.get("spread"))
    volume     = market.get("volume")
    ticker     = _prefix(symbol)

    # Update market_price in securities table
    if last_price is not None:
        import aiosqlite
        from config import settings as cfg
        total_shares_row = await db._fetchone(
            "SELECT total_shares FROM securities WHERE ticker = ?", (ticker,)
        )
        total_shares = (total_shares_row or {}).get("total_shares") or 0
        market_cap = last_price * total_shares if total_shares else None
        async with aiosqlite.connect(cfg.db_path) as conn:
            await conn.execute(
                """UPDATE securities SET market_price = ?, market_cap = ?, updated_at = ?
                   WHERE ticker = ?""",
                (last_price, market_cap, datetime.now(timezone.utc).isoformat(), ticker)
            )
            await conn.commit()

    # Update orderbook cache
    if best_bid is not None or best_ask is not None:
        mid = ((best_bid + best_ask) / 2) if best_bid and best_ask else (best_bid or best_ask)
        await db.upsert_orderbook(ticker, {
            "bids": [[best_bid, None]] if best_bid else [],
            "asks": [[best_ask, None]] if best_ask else [],
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid": mid,
            "spread": spread,
            "source": "tse",
        })


async def poll_all_tse_prices() -> None:
    """Poll prices + market data + orderbook for all TSE stocks."""
    if not TSE_ENABLED:
        return
    tickers = await db.get_all_tickers()
    tse_tickers = [t for t in tickers if t.startswith("TSE:")]

    for ticker in tse_tickers:
        await poll_tse_prices(ticker)      # trade history
        await poll_tse_market(ticker)      # lastPrice + bid/ask → securities + orderbook cache
        await poll_tse_orderbook(ticker)   # full depth orderbook
        await asyncio.sleep(0.1)

    await db.set_meta("tse_last_price_poll", datetime.now(timezone.utc).isoformat())
    logger.info("TSE: polled prices for %d tickers", len(tse_tickers))


async def poll_all_tse_ohlcv() -> None:
    """Poll OHLCV for all TSE stocks."""
    if not TSE_ENABLED:
        return
    tickers = await db.get_all_tickers()
    tse_tickers = [t for t in tickers if t.startswith("TSE:")]

    for ticker in tse_tickers:
        await poll_tse_ohlcv(ticker, interval="1h")
        await asyncio.sleep(0.1)

    await db.set_meta("tse_last_ohlcv_poll", datetime.now(timezone.utc).isoformat())
    logger.info("TSE: polled OHLCV for %d tickers", len(tse_tickers))


# ── Options ───────────────────────────────────────────────────────────────────

def _compute_moneyness(option_type: str, strike: float, underlying: float) -> Optional[str]:
    if not strike or not underlying:
        return None
    diff_pct = abs(strike - underlying) / underlying
    if diff_pct < 0.01:
        return "ATM"
    if option_type == "call":
        return "ITM" if underlying > strike else "OTM"
    else:  # put
        return "ITM" if underlying < strike else "OTM"


async def _enrich_option(contract_id: str) -> Optional[dict]:
    """Fetch full detail for a single options contract."""
    return await _get(f"/api/v1/options/contracts/{contract_id}")


async def poll_tse_options() -> None:
    """Fetch all options contracts (all statuses) for historical record + active monitoring."""
    if not TSE_ENABLED:
        return

    # Fetch all contracts — no status filter, we store everything
    data = await _get("/api/v1/options/contracts")
    if not data:
        return

    contracts_raw = data if isinstance(data, list) else data.get("contracts", data.get("data", []))
    logger.info("TSE options: found %d contracts", len(contracts_raw))

    def _f(v):
        try: return float(v) if v is not None else None
        except (ValueError, TypeError): return None

    for c in contracts_raw:
        contract_id = c.get("contract_id") or c.get("id")
        if not contract_id:
            continue

        underlying_symbol = c.get("symbol") or ""
        option_type = (c.get("option_type") or "call").lower()
        strike = _f(c.get("strike_price"))
        underlying_price = _f(c.get("underlying_price"))
        expiration_ts = c.get("expiration_ts") or c.get("expiration_date") or c.get("expires_at")
        status = (c.get("status") or "active").lower()
        hours = _hours_to_expiry(expiration_ts) if status == "active" else None
        moneyness = _compute_moneyness(option_type, strike, underlying_price)

        await db.upsert_options_contract({
            "contract_id": contract_id,
            "symbol": _prefix(underlying_symbol),
            "option_ticker": c.get("option_ticker"),
            "underlying_ticker": _prefix(underlying_symbol),
            "option_type": option_type,
            "strike_price": strike,
            "shares_per_contract": c.get("shares_per_contract"),
            "max_quantity": c.get("max_quantity"),
            "premium": _f(c.get("premium")),
            "expiration_ts": expiration_ts,
            "status": status,
            "current_price": _f(c.get("current_price")),
            "underlying_price": underlying_price,
            "intrinsic_value": _f(c.get("intrinsic_value")),
            "time_value": _f(c.get("time_value")),
            "theoretical_price": _f(c.get("theoretical_price")),
            "delta": _f(c.get("delta")),
            "moneyness": moneyness,
            "hours_to_expiry": hours,
            "best_bid": _f(c.get("best_bid")),
            "best_ask": _f(c.get("best_ask")),
            "volume_24h": _f(c.get("volume_24h") or c.get("volume")),
        })

    await db.set_meta("tse_last_options_poll", datetime.now(timezone.utc).isoformat())
    logger.info("TSE: stored %d options contracts", len(contracts_raw))


# ── Bonds ─────────────────────────────────────────────────────────────────────

async def poll_tse_bonds() -> None:
    """Fetch all bonds + their market data."""
    if not TSE_ENABLED:
        return

    data = await _get("/api/v1/bonds")
    if not data:
        return

    bonds_raw = data if isinstance(data, list) else data.get("bonds", data.get("data", []))
    logger.info("TSE bonds: found %d bonds", len(bonds_raw))

    for b in bonds_raw:
        bond_id = b.get("id") or b.get("bond_id")
        symbol = b.get("symbol") or b.get("ticker") or bond_id
        if not bond_id:
            continue

        # Try to fetch market data (orderbook + trades)
        market = await _get(f"/api/v1/market/bond/{symbol}/orderbook") or {}
        trades_data = await _get(f"/api/v1/market/bond/{symbol}/trades", params={"limit": 50}) or []
        trades = trades_data if isinstance(trades_data, list) else trades_data.get("trades", [])

        bids = market.get("bids", [])
        asks = market.get("asks", [])
        best_bid = bids[0].get("price") if bids else None
        best_ask = asks[0].get("price") if asks else None

        maturity_date = b.get("maturity_date") or b.get("matures_at")
        days_to_maturity = None
        if maturity_date:
            try:
                mat = datetime.fromisoformat(str(maturity_date).replace("Z", "+00:00"))
                days_to_maturity = max(0, (mat - datetime.now(timezone.utc)).days)
            except Exception:
                pass

        await db.upsert_bond({
            "bond_id": bond_id,
            "symbol": symbol,
            "issuer_name": b.get("issuer") or b.get("issuer_name"),
            "bond_type": b.get("type") or b.get("bond_type"),
            "face_value": b.get("face_value") or b.get("par_value"),
            "coupon_rate": b.get("coupon_rate"),
            "coupon_frequency": b.get("coupon_frequency"),
            "maturity_date": maturity_date,
            "status": b.get("status", "active"),
            "current_price": b.get("current_price") or b.get("price") or best_bid,
            "yield_to_maturity": b.get("yield_to_maturity") or b.get("ytm"),
            "accrued_interest": b.get("accrued_interest"),
            "dirty_price": b.get("dirty_price"),
            "days_to_maturity": days_to_maturity,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "volume_24h": b.get("volume_24h") or b.get("volume"),
        })

        # Insert trade history
        if trades:
            records = [{"price": t.get("price"), "volume": t.get("quantity") or t.get("volume"),
                        "timestamp": t.get("timestamp") or t.get("created_at")} for t in trades]
            await db.insert_bond_price_history(bond_id, symbol, records)

        await asyncio.sleep(0.1)

    await db.set_meta("tse_last_bonds_poll", datetime.now(timezone.utc).isoformat())
    logger.info("TSE: polled %d bonds", len(bonds_raw))


# ── Prediction contracts ──────────────────────────────────────────────────────

def _implied_prob(yes_price: float, no_price: float) -> Optional[float]:
    """CPMM implied probability from yes price."""
    if yes_price is None:
        return None
    # In a binary prediction market, yes_price ≈ probability
    try:
        return round(float(yes_price), 4)
    except Exception:
        return None


async def poll_tse_prediction_contracts() -> None:
    """Fetch all active prediction/event contracts."""
    if not TSE_ENABLED:
        return

    data = await _get("/api/v1/contracts", params={"status": "active"})
    if not data:
        return

    contracts_raw = data if isinstance(data, list) else data.get("contracts", data.get("data", []))
    logger.info("TSE predictions: found %d active contracts", len(contracts_raw))

    for c in contracts_raw:
        contract_id = c.get("id") or c.get("contract_id")
        if not contract_id:
            continue

        expiration_ts = c.get("expiration_date") or c.get("expires_at") or c.get("resolution_date")
        hours = _hours_to_expiry(expiration_ts)
        yes_price = c.get("yes_price") or c.get("yes")
        no_price = c.get("no_price") or c.get("no")
        implied_prob = _implied_prob(yes_price, no_price)

        await db.upsert_prediction_contract({
            "contract_id": contract_id,
            "title": c.get("title") or c.get("question"),
            "description": c.get("description"),
            "status": c.get("status", "active"),
            "outcome": c.get("outcome") or c.get("result"),
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_reserves": c.get("yes_reserves") or c.get("pool_yes"),
            "no_reserves": c.get("no_reserves") or c.get("pool_no"),
            "volume_24h": c.get("volume_24h"),
            "total_volume": c.get("total_volume") or c.get("volume"),
            "expiration_ts": expiration_ts,
            "implied_prob_yes": implied_prob,
            "hours_to_expiry": hours,
        })

    await db.set_meta("tse_last_contracts_poll", datetime.now(timezone.utc).isoformat())
    logger.info("TSE: polled %d prediction contracts", len(contracts_raw))


# ── Dynamic options poll frequency ───────────────────────────────────────────

async def poll_urgent_options() -> None:
    """
    Re-poll options expiring within 24h more aggressively.
    Called every 30s by a separate scheduler job when such contracts exist.
    """
    if not TSE_ENABLED:
        return
    soon = datetime.now(timezone.utc) + timedelta(hours=24)
    soon_str = soon.isoformat()

    import aiosqlite
    from config import settings as cfg
    async with aiosqlite.connect(cfg.db_path) as conn:
        conn.row_factory = aiosqlite.Row
        cur = await conn.execute(
            "SELECT contract_id FROM options_contracts "
            "WHERE status = 'active' AND expiration_ts IS NOT NULL AND expiration_ts <= ?",
            (soon_str,)
        )
        rows = await cur.fetchall()

    for row in rows:
        detail = await _enrich_option(row["contract_id"])
        if detail:
            contract_id = detail.get("id") or detail.get("contract_id") or row["contract_id"]
            # Re-upsert with fresh data — same mapping as poll_tse_options
            underlying_symbol = detail.get("underlying_symbol") or detail.get("symbol") or ""
            strike = detail.get("strike_price") or detail.get("strike")
            underlying_price = detail.get("underlying_price") or detail.get("spot_price")
            option_type = (detail.get("option_type") or "call").lower()
            expiration_ts = detail.get("expiration_date") or detail.get("expires_at")
            hours = _hours_to_expiry(expiration_ts)
            moneyness = _compute_moneyness(option_type, strike, underlying_price)

            await db.upsert_options_contract({
                "contract_id": contract_id,
                "symbol": _prefix(underlying_symbol),
                "underlying_ticker": _prefix(underlying_symbol),
                "option_type": option_type,
                "strike_price": strike,
                "shares_per_contract": detail.get("shares_per_contract"),
                "expiration_ts": expiration_ts,
                "status": detail.get("status", "active"),
                "current_price": detail.get("current_price") or detail.get("price"),
                "underlying_price": underlying_price,
                "intrinsic_value": detail.get("intrinsic_value"),
                "time_value": detail.get("time_value"),
                "theoretical_price": detail.get("theoretical_price"),
                "delta": detail.get("delta"),
                "moneyness": moneyness,
                "hours_to_expiry": hours,
                "best_bid": detail.get("best_bid"),
                "best_ask": detail.get("best_ask"),
                "volume_24h": detail.get("volume_24h"),
            })
        await asyncio.sleep(0.05)

    if rows:
        logger.info("TSE urgent poll: refreshed %d near-expiry options", len(rows))
