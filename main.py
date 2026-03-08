"""
Atlas — Main Entrypoint
Starts the database, ingestion scheduler, and FastAPI server together.
"""

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

import database as db
import ingestion
import ingestion_tse
from api.routes import router
from dashboard import dashboard_router
from admin import admin_router
from config import settings

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("atlas")

# ── App ───────────────────────────────────────────────────────────────────────
# ── Lifecycle ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──
    scheduler = None
    logger.info("Atlas starting up...")

    # 1. Initialize database (auto-creates directory if needed)
    await db.init_db()
    logger.info("Database ready")

    # 2. Initialize HTTP session for NER API calls
    await ingestion.init_session()

    # 3. Start serving immediately, run initial sync in background
    asyncio.create_task(ingestion.run_initial_sync())

    # 4. Start the polling scheduler
    scheduler = ingestion.create_scheduler()

    # 5. Register TSE jobs if TSE_API_KEY is set
    if settings.tse_api_key:
        logger.info("TSE API key detected — registering TSE polling jobs")
        scheduler.add_job(ingestion_tse.poll_tse_securities, "interval",
                          seconds=settings.poll_tse_securities_interval,
                          id="tse_securities", max_instances=1, misfire_grace_time=30)
        scheduler.add_job(ingestion_tse.poll_all_tse_prices, "interval",
                          seconds=settings.poll_tse_price_interval,
                          id="tse_prices", max_instances=1, misfire_grace_time=30)
        scheduler.add_job(ingestion_tse.poll_all_tse_ohlcv, "interval",
                          seconds=settings.poll_tse_ohlcv_interval,
                          id="tse_ohlcv", max_instances=1, misfire_grace_time=60)
        scheduler.add_job(ingestion_tse.poll_tse_options, "interval",
                          seconds=settings.poll_tse_options_interval,
                          id="tse_options", max_instances=1, misfire_grace_time=60)
        scheduler.add_job(ingestion_tse.poll_tse_bonds, "interval",
                          seconds=settings.poll_tse_bonds_interval,
                          id="tse_bonds", max_instances=1, misfire_grace_time=60)
        scheduler.add_job(ingestion_tse.poll_tse_prediction_contracts, "interval",
                          seconds=settings.poll_tse_contracts_interval,
                          id="tse_predictions", max_instances=1, misfire_grace_time=60)
        # Expiry sweep — runs every minute for all time-constrained instruments
        scheduler.add_job(db.expire_all_stale, "interval",
                          seconds=settings.expire_contracts_interval,
                          id="expire_stale", max_instances=1, misfire_grace_time=30)
        # Urgent options re-poll (near-expiry contracts every 30s)
        scheduler.add_job(ingestion_tse.poll_urgent_options, "interval",
                          seconds=30, id="tse_options_urgent", max_instances=1, misfire_grace_time=10)
        # Initial TSE sync in background
        asyncio.create_task(_tse_initial_sync())
    else:
        logger.info("TSE_API_KEY not set — TSE polling disabled")

    scheduler.start()
    logger.info("Scheduler started with %d jobs", len(scheduler.get_jobs()))
    logger.info("Atlas is ready on port %d", settings.port)

    yield  # App is running

    # ── Shutdown ──
    logger.info("Atlas shutting down...")
    if scheduler and scheduler.running:
        scheduler.shutdown(wait=False)
    await ingestion.close_session()
    logger.info("Atlas stopped cleanly")


app = FastAPI(
    title="Atlas",
    description="Bloomberg Labs — Market Data Infrastructure",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(dashboard_router)
app.include_router(admin_router)

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/admin")


# ── CLI key management helper ─────────────────────────────────────────────────
# Run: python main.py create-key <tool_id> <tool_name>
# This lets you bootstrap the first API key before any tool can call /admin/keys

async def _tse_initial_sync() -> None:
    """Initial TSE sync — runs once at startup if TSE key is present."""
    logger.info("Running initial TSE sync...")
    await ingestion_tse.poll_tse_securities()
    await asyncio.gather(
        ingestion_tse.poll_all_tse_prices(),
        ingestion_tse.poll_tse_options(),
        ingestion_tse.poll_tse_bonds(),
        ingestion_tse.poll_tse_prediction_contracts(),
    )
    await ingestion_tse.poll_all_tse_ohlcv()
    logger.info("Initial TSE sync complete")


async def _cli_create_key(tool_id: str, tool_name: str):
    await db.init_db()
    from auth import create_tool_key
    try:
        key = await create_tool_key(tool_id, tool_name)
        print(f"\n✓ Created key for '{tool_name}' (ID: {tool_id})")
        print(f"\n  Key: {key}\n")
        print("  Save this now — it won't be shown again.\n")
    except ValueError as e:
        print(f"\n✗ Error: {e}\n")


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "create-key":
        asyncio.run(_cli_create_key(sys.argv[2], sys.argv[3]))
    else:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=settings.port,
            log_level=settings.log_level.lower(),
        )
