"""
Atlas Database Layer
Handles all SQLite operations. Two patterns per data type:
  - live_cache: single row per ticker, fast reads, overwritten on each poll
  - history tables: append-only time series for derived metric computation
"""

import json
import logging
import aiosqlite
from datetime import datetime, timezone
from typing import Any, Optional

from config import settings

logger = logging.getLogger(__name__)

DB = settings.db_path


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- All known securities (live cache, one row per ticker)
-- ticker is prefixed: NER:XXX or TSE:XXX
CREATE TABLE IF NOT EXISTS securities (
    ticker              TEXT PRIMARY KEY,
    full_name           TEXT,
    market_price        REAL,
    total_shares        INTEGER,
    market_cap          REAL,
    shareholder_count   INTEGER,
    frozen              INTEGER DEFAULT 0,
    hidden              INTEGER DEFAULT 0,
    security_type       TEXT,
    source              TEXT DEFAULT 'ner',   -- 'ner' | 'tse'
    updated_at          TEXT
);

-- Latest orderbook per ticker (overwritten on each update)
CREATE TABLE IF NOT EXISTS orderbook_cache (
    ticker          TEXT PRIMARY KEY,
    bids            TEXT,   -- JSON
    asks            TEXT,   -- JSON
    best_bid        REAL,
    best_ask        REAL,
    mid             REAL,
    source          TEXT DEFAULT 'ner',
    captured_at     TEXT
);

-- Orderbook history (append-only for imbalance/depth trend analysis)
CREATE TABLE IF NOT EXISTS orderbook_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker          TEXT NOT NULL,
    bids            TEXT,
    asks            TEXT,
    best_bid        REAL,
    best_ask        REAL,
    mid             REAL,
    source          TEXT DEFAULT 'ner',
    captured_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ob_hist ON orderbook_history(ticker, captured_at);

-- Trade-level price history (append-only, sourced from NER + webhooks + TSE)
CREATE TABLE IF NOT EXISTS price_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    price       REAL NOT NULL,
    volume      INTEGER,
    timestamp   TEXT NOT NULL,
    source      TEXT DEFAULT 'ner',
    UNIQUE(ticker, timestamp, price, source)
);
CREATE INDEX IF NOT EXISTS idx_ph ON price_history(ticker, timestamp);

-- Daily OHLCV candles
CREATE TABLE IF NOT EXISTS ohlcv (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    date        TEXT NOT NULL,
    open        REAL,
    high        REAL,
    low         REAL,
    close       REAL,
    volume      INTEGER,
    source      TEXT DEFAULT 'ner',
    interval    TEXT DEFAULT '1d',
    UNIQUE(ticker, date, source, interval)
);
CREATE INDEX IF NOT EXISTS idx_ohlcv ON ohlcv(ticker, date);

-- Shareholder snapshots (latest per ticker/user)
CREATE TABLE IF NOT EXISTS shareholders (
    ticker      TEXT NOT NULL,
    user_id     TEXT NOT NULL,
    quantity    INTEGER,
    cost_basis  REAL,
    source      TEXT DEFAULT 'ner',
    updated_at  TEXT,
    PRIMARY KEY (ticker, user_id)
);

-- Financial stats per ticker
CREATE TABLE IF NOT EXISTS security_stats (
    ticker      TEXT PRIMARY KEY,
    eps         REAL,
    pe_ratio    REAL,
    pb_ratio    REAL,
    roa_percent REAL,
    book_value  REAL,
    net_profit  REAL,
    updated_at  TEXT
);

-- Computed derived metrics (one row per ticker, recomputed after each ingestion)
CREATE TABLE IF NOT EXISTS derived_metrics (
    ticker                  TEXT PRIMARY KEY,
    vwap_7d                 REAL,
    vwap_24h                REAL,
    volatility_7d           REAL,
    spread                  REAL,
    spread_pct              REAL,
    bid_depth               REAL,
    ask_depth               REAL,
    orderbook_imbalance     REAL,
    liquidity_score         REAL,
    source                  TEXT DEFAULT 'ner',
    last_computed_at        TEXT
);

-- TSE Options contracts
CREATE TABLE IF NOT EXISTS options_contracts (
    contract_id         TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,        -- TSE:GCC etc
    underlying_ticker   TEXT NOT NULL,
    option_type         TEXT NOT NULL,        -- 'call' | 'put'
    strike_price        REAL,
    shares_per_contract INTEGER,
    expiration_ts       TEXT,
    status              TEXT DEFAULT 'active', -- 'active' | 'expired' | 'settled'
    current_price       REAL,
    underlying_price    REAL,
    intrinsic_value     REAL,
    time_value          REAL,
    theoretical_price   REAL,
    delta               REAL,
    -- computed enrichment
    moneyness           TEXT,                 -- 'ITM' | 'OTM' | 'ATM'
    hours_to_expiry     REAL,
    -- market data
    best_bid            REAL,
    best_ask            REAL,
    volume_24h          REAL,
    last_polled_at      TEXT,
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_options_symbol ON options_contracts(symbol, status);
CREATE INDEX IF NOT EXISTS idx_options_expiry ON options_contracts(expiration_ts, status);

-- TSE Bonds
CREATE TABLE IF NOT EXISTS bonds (
    bond_id             TEXT PRIMARY KEY,
    symbol              TEXT NOT NULL,
    issuer_name         TEXT,
    bond_type           TEXT,                 -- 'treasury' | 'corporate' | 'municipal' | 'government'
    face_value          REAL,
    coupon_rate         REAL,
    coupon_frequency    TEXT,
    maturity_date       TEXT,
    status              TEXT DEFAULT 'active', -- 'active' | 'matured' | 'defaulted'
    current_price       REAL,
    yield_to_maturity   REAL,
    accrued_interest    REAL,
    dirty_price         REAL,
    days_to_maturity    INTEGER,
    best_bid            REAL,
    best_ask            REAL,
    volume_24h          REAL,
    last_polled_at      TEXT,
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_bonds_symbol ON bonds(symbol, status);

-- TSE Bond price history (like price_history but for bonds)
CREATE TABLE IF NOT EXISTS bond_price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bond_id         TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    price           REAL NOT NULL,
    volume          INTEGER,
    timestamp       TEXT NOT NULL,
    UNIQUE(bond_id, timestamp, price)
);
CREATE INDEX IF NOT EXISTS idx_bond_ph ON bond_price_history(bond_id, timestamp);

-- TSE Prediction/event contracts
CREATE TABLE IF NOT EXISTS prediction_contracts (
    contract_id         TEXT PRIMARY KEY,
    title               TEXT,
    description         TEXT,
    status              TEXT DEFAULT 'active', -- 'active' | 'resolved' | 'cancelled'
    outcome             TEXT,                  -- null until resolved
    yes_price           REAL,
    no_price            REAL,
    yes_reserves        REAL,
    no_reserves         REAL,
    volume_24h          REAL,
    total_volume        REAL,
    expiration_ts       TEXT,
    -- computed
    implied_prob_yes    REAL,
    hours_to_expiry     REAL,
    last_polled_at      TEXT,
    updated_at          TEXT
);
CREATE INDEX IF NOT EXISTS idx_pred_status ON prediction_contracts(status);
CREATE INDEX IF NOT EXISTS idx_pred_expiry ON prediction_contracts(expiration_ts, status);

-- Per-tool API keys for Atlas authentication
CREATE TABLE IF NOT EXISTS api_keys (
    key_id      TEXT PRIMARY KEY,        -- e.g. "atl_bloomberg_terminal"
    key_hash    TEXT NOT NULL UNIQUE,    -- sha256 of the actual key
    tool_name   TEXT NOT NULL,           -- human label e.g. "Bloomberg Terminal"
    created_at  TEXT NOT NULL,
    last_used   TEXT,
    active      INTEGER DEFAULT 1
);

-- Per-request log for API key usage statistics
CREATE TABLE IF NOT EXISTS request_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id      TEXT NOT NULL,
    endpoint    TEXT NOT NULL,
    method      TEXT NOT NULL DEFAULT 'GET',
    status_code INTEGER,
    ts          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_request_log_key ON request_log(key_id);
CREATE INDEX IF NOT EXISTS idx_request_log_ts  ON request_log(ts);

-- Atlas operational metadata (last poll times, etc.)
CREATE TABLE IF NOT EXISTS atlas_meta (
    key     TEXT PRIMARY KEY,
    value   TEXT
);
"""


async def init_db() -> None:
    """Create all tables if they don't exist. Auto-creates parent directory."""
    import os
    db_dir = os.path.dirname(DB)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
        logger.info("Created database directory: %s", db_dir)
    async with aiosqlite.connect(DB) as conn:
        await conn.executescript(SCHEMA)
        await conn.commit()

        # Migration: rebuild price_history with UNIQUE constraint if not present
        cur = await conn.execute("PRAGMA index_list(price_history)")
        indexes = [row[1] for row in await cur.fetchall()]
        if "sqlite_autoindex_price_history_1" not in indexes:
            logger.info("Migrating price_history: adding UNIQUE constraint and removing duplicates...")
            await conn.executescript("""
                CREATE TABLE IF NOT EXISTS price_history_new (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker    TEXT NOT NULL,
                    price     REAL NOT NULL,
                    volume    INTEGER,
                    timestamp TEXT NOT NULL,
                    source    TEXT DEFAULT 'ner',
                    UNIQUE(ticker, timestamp, price, source)
                );
                INSERT OR IGNORE INTO price_history_new(ticker, price, volume, timestamp, source)
                    SELECT ticker, price, volume, timestamp, COALESCE(source, 'ner') FROM price_history;
                DROP TABLE price_history;
                ALTER TABLE price_history_new RENAME TO price_history;
                CREATE INDEX IF NOT EXISTS idx_ph ON price_history(ticker, timestamp);
            """)
            await conn.commit()
            logger.info("Migration complete: price_history deduplicated.")

        # Migration: add source column to tables that may not have it yet
        for table in ["securities", "orderbook_cache", "orderbook_history", "ohlcv", "shareholders", "derived_metrics"]:
            cur = await conn.execute(f"PRAGMA table_info({table})")
            cols = [row[1] for row in await cur.fetchall()]
            if "source" not in cols:
                logger.info("Migrating %s: adding source column...", table)
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN source TEXT DEFAULT 'ner'")
                await conn.commit()

        # Migration: add interval column to ohlcv if missing
        cur = await conn.execute("PRAGMA table_info(ohlcv)")
        cols = [row[1] for row in await cur.fetchall()]
        if "interval" not in cols:
            logger.info("Migrating ohlcv: adding interval column...")
            await conn.execute("ALTER TABLE ohlcv ADD COLUMN interval TEXT DEFAULT '1d'")
            await conn.commit()

    logger.info("Database initialized: %s", DB)


# ── Generic helpers ────────────────────────────────────────────────────────────

async def _fetchall(query: str, params: tuple = ()) -> list[dict]:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def _fetchone(query: str, params: tuple = ()) -> Optional[dict]:
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row else None


async def _execute(query: str, params: tuple = ()) -> None:
    async with aiosqlite.connect(DB) as db:
        await db.execute(query, params)
        await db.commit()


async def _executemany(query: str, params_list: list[tuple]) -> None:
    async with aiosqlite.connect(DB) as db:
        await db.executemany(query, params_list)
        await db.commit()


# ── Meta (last poll timestamps) ───────────────────────────────────────────────

async def set_meta(key: str, value: str) -> None:
    await _execute(
        "INSERT OR REPLACE INTO atlas_meta(key, value) VALUES (?, ?)",
        (key, value)
    )


async def get_meta(key: str) -> Optional[str]:
    row = await _fetchone("SELECT value FROM atlas_meta WHERE key = ?", (key,))
    return row["value"] if row else None


# ── Securities ────────────────────────────────────────────────────────────────

async def upsert_securities(securities: list[dict]) -> None:
    now = _now()
    await _executemany(
        """INSERT OR REPLACE INTO securities
           (ticker, full_name, market_price, total_shares, market_cap,
            shareholder_count, frozen, hidden, security_type, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [(
            s.get("ticker"),
            s.get("full_name"),
            s.get("market_price"),
            s.get("total_shares"),
            s.get("market_cap"),
            s.get("shareholder_count"),
            int(s.get("frozen", False)),
            int(s.get("hidden", False)),
            s.get("security_type"),
            now,
        ) for s in securities]
    )


async def update_market_price(ticker: str, price: float, frozen: bool) -> None:
    await _execute(
        "UPDATE securities SET market_price = ?, frozen = ?, updated_at = ? WHERE ticker = ?",
        (price, int(frozen), _now(), ticker)
    )


async def get_all_securities() -> list[dict]:
    return await _fetchall("SELECT * FROM securities WHERE hidden = 0 ORDER BY ticker")


async def get_security(ticker: str) -> Optional[dict]:
    return await _fetchone("SELECT * FROM securities WHERE ticker = ?", (ticker,))


async def get_all_tickers() -> list[str]:
    rows = await _fetchall("SELECT ticker FROM securities WHERE hidden = 0")
    return [r["ticker"] for r in rows]


# ── Orderbook ─────────────────────────────────────────────────────────────────

async def upsert_orderbook(ticker: str, data: dict) -> None:
    now = _now()
    bids = json.dumps(data.get("bids", []))
    asks = json.dumps(data.get("asks", []))
    source = data.get("source", "ner")
    await _execute(
        """INSERT OR REPLACE INTO orderbook_cache
           (ticker, bids, asks, best_bid, best_ask, mid, source, captured_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, bids, asks, data.get("best_bid"), data.get("best_ask"), data.get("mid"), source, now)
    )
    # Also append to history
    await _execute(
        """INSERT INTO orderbook_history
           (ticker, bids, asks, best_bid, best_ask, mid, source, captured_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, bids, asks, data.get("best_bid"), data.get("best_ask"), data.get("mid"), source, now)
    )


async def get_orderbook(ticker: str) -> Optional[dict]:
    row = await _fetchone("SELECT * FROM orderbook_cache WHERE ticker = ?", (ticker,))
    if not row:
        return None
    row["bids"] = json.loads(row["bids"] or "[]")
    row["asks"] = json.loads(row["asks"] or "[]")
    return row


async def get_all_orderbooks() -> list[dict]:
    rows = await _fetchall("SELECT * FROM orderbook_cache")
    for r in rows:
        r["bids"] = json.loads(r["bids"] or "[]")
        r["asks"] = json.loads(r["asks"] or "[]")
    return rows


async def get_orderbook_history(ticker: str | None = None, limit: int = 500) -> list[dict]:
    if ticker:
        return await _fetchall(
            "SELECT * FROM orderbook_history WHERE ticker = ? ORDER BY captured_at DESC LIMIT ?",
            (ticker.upper(), limit)
        )
    return await _fetchall(
        "SELECT * FROM orderbook_history ORDER BY captured_at DESC LIMIT ?",
        (limit,)
    )


# ── Price history ─────────────────────────────────────────────────────────────

def _normalize_timestamp(ts: str | None) -> str | None:
    """Normalize any timestamp format to ISO 8601."""
    if not ts:
        return None
    ts = str(ts).strip()
    # Already ISO
    if "T" in ts or len(ts) > 10:
        return ts
    # dd-mm-yy → ISO
    try:
        from datetime import datetime
        for fmt in ("%d-%m-%y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(ts, fmt).strftime("%Y-%m-%dT00:00:00")
            except ValueError:
                continue
    except Exception:
        pass
    return ts


async def insert_price_history(ticker: str, records: list[dict], source: str = "ner_api") -> None:
    """Insert price history, skipping duplicates via UNIQUE(ticker, timestamp, price)."""
    if not records:
        return
    await _executemany(
        """INSERT OR IGNORE INTO price_history(ticker, price, volume, timestamp, source)
           VALUES (?, ?, ?, ?, ?)""",
        [(ticker, r.get("price"), r.get("volume"), _normalize_timestamp(r.get("timestamp")), source)
         for r in records]
    )


async def get_price_history(ticker: str, days: int = 30, limit: int = 500,
                             from_dt: str = None, to_dt: str = None) -> list[dict]:
    if from_dt and to_dt:
        return await _fetchall(
            "SELECT * FROM price_history WHERE ticker = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT ?",
            (ticker, from_dt, to_dt, limit)
        )
    return await _fetchall(
        """SELECT * FROM price_history WHERE ticker = ?
           AND timestamp >= datetime('now', ?)
           ORDER BY timestamp DESC LIMIT ?""",
        (ticker, f"-{days} days", limit)
    )


async def get_all_price_history(limit: int = 100, since: str = None) -> list[dict]:
    """All tickers, ordered by timestamp DESC. Used by /transactions."""
    if since:
        return await _fetchall(
            "SELECT * FROM price_history WHERE timestamp >= ? ORDER BY timestamp DESC LIMIT ?",
            (since, limit)
        )
    return await _fetchall(
        "SELECT * FROM price_history ORDER BY timestamp DESC LIMIT ?",
        (limit,)
    )


# ── OHLCV ─────────────────────────────────────────────────────────────────────

async def upsert_ohlcv(ticker: str, candles: list[dict]) -> None:
    await _executemany(
        """INSERT OR REPLACE INTO ohlcv(ticker, date, open, high, low, close, volume)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [(ticker, c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"])
         for c in candles]
    )


async def get_ohlcv(ticker: str, days: int = 30) -> list[dict]:
    return await _fetchall(
        """SELECT * FROM ohlcv WHERE ticker = ?
           AND date >= date('now', ?)
           ORDER BY date DESC""",
        (ticker, f"-{days} days")
    )


# ── Shareholders ──────────────────────────────────────────────────────────────

async def upsert_shareholders(ticker: str, shareholders: list[dict]) -> None:
    now = _now()
    # Clear old entries for this ticker then re-insert
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM shareholders WHERE ticker = ?", (ticker,))
        await db.executemany(
            "INSERT INTO shareholders(ticker, user_id, quantity, cost_basis, updated_at) VALUES (?, ?, ?, ?, ?)",
            [(ticker, s["user_id"], s["quantity"], s.get("cost_basis"), now)
             for s in shareholders]
        )
        await db.commit()


async def get_shareholders(ticker: str) -> list[dict]:
    return await _fetchall(
        "SELECT * FROM shareholders WHERE ticker = ? ORDER BY quantity DESC",
        (ticker,)
    )


# ── Security stats ────────────────────────────────────────────────────────────

async def upsert_stats(ticker: str, stats: dict) -> None:
    await _execute(
        """INSERT OR REPLACE INTO security_stats
           (ticker, eps, pe_ratio, pb_ratio, roa_percent, book_value, net_profit, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, stats.get("eps"), stats.get("pe_ratio"), stats.get("pb_ratio"),
         stats.get("roa_percent"), stats.get("book_value"), stats.get("net_profit"), _now())
    )


async def get_stats(ticker: str) -> Optional[dict]:
    return await _fetchone("SELECT * FROM security_stats WHERE ticker = ?", (ticker,))


# ── Derived metrics ───────────────────────────────────────────────────────────

async def upsert_derived(ticker: str, metrics: dict) -> None:
    await _execute(
        """INSERT OR REPLACE INTO derived_metrics
           (ticker, vwap_7d, vwap_24h, volatility_7d, spread, spread_pct,
            bid_depth, ask_depth, orderbook_imbalance, liquidity_score, last_computed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (ticker, metrics.get("vwap_7d"), metrics.get("vwap_24h"),
         metrics.get("volatility_7d"), metrics.get("spread"), metrics.get("spread_pct"),
         metrics.get("bid_depth"), metrics.get("ask_depth"),
         metrics.get("orderbook_imbalance"), metrics.get("liquidity_score"), _now())
    )


async def get_derived(ticker: str) -> Optional[dict]:
    return await _fetchone("SELECT * FROM derived_metrics WHERE ticker = ?", (ticker,))


async def get_all_derived() -> list[dict]:
    return await _fetchall("SELECT * FROM derived_metrics ORDER BY ticker")


# ── API keys ──────────────────────────────────────────────────────────────────

async def create_api_key(key_id: str, key_hash: str, tool_name: str) -> None:
    await _execute(
        "INSERT INTO api_keys(key_id, key_hash, tool_name, created_at) VALUES (?, ?, ?, ?)",
        (key_id, key_hash, tool_name, _now())
    )


async def get_api_key_by_hash(key_hash: str) -> Optional[dict]:
    return await _fetchone(
        "SELECT * FROM api_keys WHERE key_hash = ? AND active = 1",
        (key_hash,)
    )


async def touch_api_key(key_id: str) -> None:
    await _execute(
        "UPDATE api_keys SET last_used = ? WHERE key_id = ?",
        (_now(), key_id)
    )


async def list_api_keys() -> list[dict]:
    return await _fetchall("SELECT key_id, tool_name, created_at, last_used, active FROM api_keys")


async def deactivate_api_key(key_id: str) -> None:
    await _execute("UPDATE api_keys SET active = 0 WHERE key_id = ?", (key_id,))


async def log_request(key_id: str, endpoint: str, method: str = "GET", status_code: int = 200) -> None:
    await _execute(
        "INSERT INTO request_log(key_id, endpoint, method, status_code, ts) VALUES (?, ?, ?, ?, ?)",
        (key_id, endpoint, method, status_code, _now())
    )


async def get_key_stats(key_id: str) -> dict:
    """Return usage statistics for a single API key."""
    async with aiosqlite.connect(DB) as conn:
        conn.row_factory = aiosqlite.Row
        # Total requests
        cur = await conn.execute("SELECT COUNT(*) as n FROM request_log WHERE key_id = ?", (key_id,))
        total = (await cur.fetchone())["n"]

        # Requests last 24h
        cur = await conn.execute(
            "SELECT COUNT(*) as n FROM request_log WHERE key_id = ? AND ts >= datetime('now', '-1 day')", (key_id,))
        last_24h = (await cur.fetchone())["n"]

        # Requests last 7d
        cur = await conn.execute(
            "SELECT COUNT(*) as n FROM request_log WHERE key_id = ? AND ts >= datetime('now', '-7 days')", (key_id,))
        last_7d = (await cur.fetchone())["n"]

        # Top endpoints
        cur = await conn.execute(
            "SELECT endpoint, COUNT(*) as n FROM request_log WHERE key_id = ? GROUP BY endpoint ORDER BY n DESC LIMIT 10",
            (key_id,))
        top_endpoints = [dict(r) for r in await cur.fetchall()]

        # Requests per day (last 14 days)
        cur = await conn.execute(
            "SELECT DATE(ts) as day, COUNT(*) as n FROM request_log WHERE key_id = ? AND ts >= datetime('now', '-14 days') GROUP BY day ORDER BY day",
            (key_id,))
        per_day = [dict(r) for r in await cur.fetchall()]

        # Recent requests
        cur = await conn.execute(
            "SELECT endpoint, method, status_code, ts FROM request_log WHERE key_id = ? ORDER BY ts DESC LIMIT 50",
            (key_id,))
        recent = [dict(r) for r in await cur.fetchall()]

    return {
        "total": total,
        "last_24h": last_24h,
        "last_7d": last_7d,
        "top_endpoints": top_endpoints,
        "per_day": per_day,
        "recent": recent,
    }


# ── DB stats (for /status endpoint) ──────────────────────────────────────────

async def get_db_stats() -> dict:
    counts = {}
    for table in ["securities", "price_history", "ohlcv", "orderbook_history", "shareholders",
                  "options_contracts", "bonds", "prediction_contracts"]:
        row = await _fetchone(f"SELECT COUNT(*) as n FROM {table}")
        counts[table] = row["n"] if row else 0
    return counts


# ── TSE Securities (source-aware wrappers) ────────────────────────────────────

async def upsert_securities_with_source(securities: list[dict], source: str) -> None:
    now = _now()
    await _executemany(
        """INSERT OR REPLACE INTO securities
           (ticker, full_name, market_price, total_shares, market_cap,
            shareholder_count, frozen, hidden, security_type, source, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [(
            s.get("ticker"),
            s.get("full_name"),
            s.get("market_price"),
            s.get("total_shares"),
            s.get("market_cap"),
            s.get("shareholder_count"),
            int(s.get("frozen", False)),
            int(s.get("hidden", False)),
            s.get("security_type"),
            source,
            now,
        ) for s in securities]
    )


async def get_all_securities_by_source(source: str = None) -> list[dict]:
    if source:
        return await _fetchall(
            "SELECT * FROM securities WHERE hidden = 0 AND source = ? ORDER BY ticker",
            (source,)
        )
    return await _fetchall("SELECT * FROM securities WHERE hidden = 0 ORDER BY ticker")


# ── Options contracts ─────────────────────────────────────────────────────────

async def upsert_options_contract(c: dict) -> None:
    now = _now()
    await _execute(
        """INSERT OR REPLACE INTO options_contracts
           (contract_id, symbol, underlying_ticker, option_type, strike_price,
            shares_per_contract, expiration_ts, status, current_price, underlying_price,
            intrinsic_value, time_value, theoretical_price, delta,
            moneyness, hours_to_expiry, best_bid, best_ask, volume_24h,
            last_polled_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            c["contract_id"], c.get("symbol"), c.get("underlying_ticker"),
            c.get("option_type"), c.get("strike_price"), c.get("shares_per_contract"),
            c.get("expiration_ts"), c.get("status", "active"),
            c.get("current_price"), c.get("underlying_price"),
            c.get("intrinsic_value"), c.get("time_value"),
            c.get("theoretical_price"), c.get("delta"),
            c.get("moneyness"), c.get("hours_to_expiry"),
            c.get("best_bid"), c.get("best_ask"), c.get("volume_24h"),
            now, now,
        )
    )


async def get_options_contract(contract_id: str) -> Optional[dict]:
    return await _fetchone(
        "SELECT * FROM options_contracts WHERE contract_id = ?", (contract_id,)
    )


async def get_all_options_contracts(active_only: bool = True, symbol: str = None,
                                     option_type: str = None) -> list[dict]:
    conditions = []
    params = []
    if active_only:
        conditions.append("status = 'active'")
    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol.upper())
    if option_type:
        conditions.append("option_type = ?")
        params.append(option_type.lower())
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return await _fetchall(
        f"SELECT * FROM options_contracts {where} ORDER BY expiration_ts ASC",
        tuple(params)
    )


async def expire_stale_options() -> int:
    """Mark options past expiration_ts as expired. Returns count updated."""
    now = _now()
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "UPDATE options_contracts SET status = 'expired' "
            "WHERE status = 'active' AND expiration_ts IS NOT NULL AND expiration_ts < ?",
            (now,)
        )
        await db.commit()
        return cur.rowcount


# ── Bonds ─────────────────────────────────────────────────────────────────────

async def upsert_bond(b: dict) -> None:
    now = _now()
    await _execute(
        """INSERT OR REPLACE INTO bonds
           (bond_id, symbol, issuer_name, bond_type, face_value, coupon_rate,
            coupon_frequency, maturity_date, status, current_price, yield_to_maturity,
            accrued_interest, dirty_price, days_to_maturity,
            best_bid, best_ask, volume_24h, last_polled_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            b["bond_id"], b.get("symbol"), b.get("issuer_name"), b.get("bond_type"),
            b.get("face_value"), b.get("coupon_rate"), b.get("coupon_frequency"),
            b.get("maturity_date"), b.get("status", "active"),
            b.get("current_price"), b.get("yield_to_maturity"),
            b.get("accrued_interest"), b.get("dirty_price"), b.get("days_to_maturity"),
            b.get("best_bid"), b.get("best_ask"), b.get("volume_24h"),
            now, now,
        )
    )


async def get_bond(bond_id: str) -> Optional[dict]:
    return await _fetchone("SELECT * FROM bonds WHERE bond_id = ?", (bond_id,))


async def get_bond_by_symbol(symbol: str) -> Optional[dict]:
    return await _fetchone(
        "SELECT * FROM bonds WHERE symbol = ? ORDER BY updated_at DESC LIMIT 1", (symbol,)
    )


async def get_all_bonds(active_only: bool = True, bond_type: str = None) -> list[dict]:
    conditions = []
    params = []
    if active_only:
        conditions.append("status = 'active'")
    if bond_type:
        conditions.append("bond_type = ?")
        params.append(bond_type.lower())
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return await _fetchall(
        f"SELECT * FROM bonds {where} ORDER BY maturity_date ASC",
        tuple(params)
    )


async def insert_bond_price_history(bond_id: str, symbol: str, records: list[dict]) -> None:
    if not records:
        return
    await _executemany(
        """INSERT OR IGNORE INTO bond_price_history(bond_id, symbol, price, volume, timestamp)
           VALUES (?, ?, ?, ?, ?)""",
        [(bond_id, symbol, r.get("price"), r.get("volume"),
          _normalize_timestamp(r.get("timestamp"))) for r in records]
    )


async def get_bond_price_history(bond_id: str, days: int = 30, limit: int = 500) -> list[dict]:
    return await _fetchall(
        """SELECT * FROM bond_price_history WHERE bond_id = ?
           AND timestamp >= datetime('now', ?)
           ORDER BY timestamp DESC LIMIT ?""",
        (bond_id, f"-{days} days", limit)
    )


async def expire_matured_bonds() -> int:
    """Mark bonds past maturity_date as matured. Returns count updated."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "UPDATE bonds SET status = 'matured' "
            "WHERE status = 'active' AND maturity_date IS NOT NULL AND maturity_date < ?",
            (today,)
        )
        await db.commit()
        return cur.rowcount


# ── Prediction contracts ──────────────────────────────────────────────────────

async def upsert_prediction_contract(c: dict) -> None:
    now = _now()
    await _execute(
        """INSERT OR REPLACE INTO prediction_contracts
           (contract_id, title, description, status, outcome,
            yes_price, no_price, yes_reserves, no_reserves,
            volume_24h, total_volume, expiration_ts,
            implied_prob_yes, hours_to_expiry, last_polled_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            c["contract_id"], c.get("title"), c.get("description"),
            c.get("status", "active"), c.get("outcome"),
            c.get("yes_price"), c.get("no_price"),
            c.get("yes_reserves"), c.get("no_reserves"),
            c.get("volume_24h"), c.get("total_volume"),
            c.get("expiration_ts"),
            c.get("implied_prob_yes"), c.get("hours_to_expiry"),
            now, now,
        )
    )


async def get_prediction_contract(contract_id: str) -> Optional[dict]:
    return await _fetchone(
        "SELECT * FROM prediction_contracts WHERE contract_id = ?", (contract_id,)
    )


async def get_all_prediction_contracts(active_only: bool = True) -> list[dict]:
    if active_only:
        return await _fetchall(
            "SELECT * FROM prediction_contracts WHERE status = 'active' ORDER BY expiration_ts ASC"
        )
    return await _fetchall("SELECT * FROM prediction_contracts ORDER BY updated_at DESC")


async def expire_stale_predictions() -> int:
    """Mark prediction contracts past expiration_ts as expired."""
    now = _now()
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute(
            "UPDATE prediction_contracts SET status = 'expired' "
            "WHERE status = 'active' AND expiration_ts IS NOT NULL AND expiration_ts < ?",
            (now,)
        )
        await db.commit()
        return cur.rowcount


# ── Combined expiry job ───────────────────────────────────────────────────────

async def expire_all_stale() -> dict:
    """Run all expiry checks. Called by APScheduler every minute."""
    opts = await expire_stale_options()
    bonds = await expire_matured_bonds()
    preds = await expire_stale_predictions()
    if opts or bonds or preds:
        logger.info("Expiry sweep: %d options, %d bonds, %d predictions expired", opts, bonds, preds)
    return {"options": opts, "bonds": bonds, "predictions": preds}
