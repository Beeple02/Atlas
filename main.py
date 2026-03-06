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

import database as db
import ingestion
from api.routes import router
from dashboard import dashboard_router
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


# ── CLI key management helper ─────────────────────────────────────────────────
# Run: python main.py create-key <tool_id> <tool_name>
# This lets you bootstrap the first API key before any tool can call /admin/keys

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
