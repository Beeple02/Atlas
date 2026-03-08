"""
Atlas Configuration
All settings are read from environment variables with sensible defaults.
Never hardcode secrets — always use environment variables.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Config:
    # ── NER API ───────────────────────────────────────────────────────────────
    ner_api_key: str = field(default_factory=lambda: os.environ["NER_API_KEY"])
    ner_base_url: str = field(default_factory=lambda: os.getenv("NER_BASE_URL", "http://150.230.117.88:8082"))

    # ── Atlas server ──────────────────────────────────────────────────────────
    port: int = field(default_factory=lambda: int(os.getenv("ATLAS_PORT") or os.getenv("PORT", "8000")))
    log_level: str = field(default_factory=lambda: os.getenv("ATLAS_LOG_LEVEL", "INFO"))

    # ── Database ──────────────────────────────────────────────────────────────
    db_path: str = field(default_factory=lambda: os.getenv("ATLAS_DB_PATH", "atlas.db"))

    # ── Webhook ───────────────────────────────────────────────────────────────
    # Must match the secret set in NER API Management config
    webhook_secret: Optional[str] = field(default_factory=lambda: os.getenv("ATLAS_WEBHOOK_SECRET"))

    # ── Polling intervals (seconds) ───────────────────────────────────────────
    poll_securities_interval: int = field(default_factory=lambda: int(os.getenv("POLL_SECURITIES_INTERVAL", "300")))    # 5 min
    poll_orderbook_interval: int = field(default_factory=lambda: int(os.getenv("POLL_ORDERBOOK_INTERVAL", "60")))       # 1 min (fallback to webhook)
    poll_price_history_interval: int = field(default_factory=lambda: int(os.getenv("POLL_PRICE_HISTORY_INTERVAL", "900")))  # 15 min
    poll_ohlcv_interval: int = field(default_factory=lambda: int(os.getenv("POLL_OHLCV_INTERVAL", "900")))             # 15 min
    poll_shareholders_interval: int = field(default_factory=lambda: int(os.getenv("POLL_SHAREHOLDERS_INTERVAL", "600")))   # 10 min
    poll_stats_interval: int = field(default_factory=lambda: int(os.getenv("POLL_STATS_INTERVAL", "600")))             # 10 min

    # Stagger delay between tickers for slow polls (seconds)
    stagger_delay: float = field(default_factory=lambda: float(os.getenv("STAGGER_DELAY", "15")))

    # ── History retention ─────────────────────────────────────────────────────
    price_history_days: int = field(default_factory=lambda: int(os.getenv("PRICE_HISTORY_DAYS", "90")))

    # ── NER API request timeout (seconds) ────────────────────────────────────
    ner_request_timeout: int = field(default_factory=lambda: int(os.getenv("NER_REQUEST_TIMEOUT", "10")))

    # ── TSE API ───────────────────────────────────────────────────────────────
    tse_api_key: Optional[str] = field(default_factory=lambda: os.getenv("TSE_API_KEY"))
    tse_base_url: str = field(default_factory=lambda: os.getenv("TSE_BASE_URL", "https://exchange.tse.gg"))

    # ── TSE polling intervals (seconds) ──────────────────────────────────────
    poll_tse_securities_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_SECURITIES_INTERVAL", "300")))   # 5 min
    poll_tse_price_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_PRICE_INTERVAL", "60")))              # 1 min
    poll_tse_orderbook_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_ORDERBOOK_INTERVAL", "60")))      # 1 min
    poll_tse_ohlcv_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_OHLCV_INTERVAL", "300")))             # 5 min
    poll_tse_options_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_OPTIONS_INTERVAL", "120")))         # 2 min
    poll_tse_bonds_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_BONDS_INTERVAL", "120")))             # 2 min
    poll_tse_contracts_interval: int = field(default_factory=lambda: int(os.getenv("POLL_TSE_CONTRACTS_INTERVAL", "120")))     # 2 min
    expire_contracts_interval: int = field(default_factory=lambda: int(os.getenv("EXPIRE_CONTRACTS_INTERVAL", "60")))          # 1 min

    # ── TSE API request timeout (seconds) ────────────────────────────────────
    tse_request_timeout: int = field(default_factory=lambda: int(os.getenv("TSE_REQUEST_TIMEOUT", "10")))


# Singleton — import this everywhere
settings = Config()
