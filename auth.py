"""
Atlas Authentication
Per-tool API key management using sha256 hashing.
Keys are stored as hashes — the plaintext key is only shown once at creation.
"""

import hashlib
import logging
import secrets
import string
from typing import Optional

from fastapi import Header, HTTPException, status

import database as db

logger = logging.getLogger(__name__)


def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


def generate_key(tool_id: str) -> str:
    """Generate a new Atlas API key. Format: atl_<tool_id>_<random>"""
    alphabet = string.ascii_letters + string.digits
    random_part = "".join(secrets.choice(alphabet) for _ in range(32))
    return f"atl_{tool_id}_{random_part}"


async def create_tool_key(tool_id: str, tool_name: str) -> str:
    """
    Create a new API key for a tool. Returns the plaintext key (shown once).
    Raises ValueError if tool_id already exists.
    """
    existing = await db.list_api_keys()
    if any(k["key_id"] == tool_id for k in existing):
        raise ValueError(f"Tool ID '{tool_id}' already has a key. Revoke it first.")
    key = generate_key(tool_id)
    await db.create_api_key(tool_id, _hash_key(key), tool_name)
    logger.info("Created API key for tool: %s (%s)", tool_id, tool_name)
    return key


async def validate_key(key: str) -> Optional[dict]:
    """Validate a key. Returns the key record or None if invalid."""
    if not key or not key.startswith("atl_"):
        return None
    record = await db.get_api_key_by_hash(_hash_key(key))
    if record:
        await db.touch_api_key(record["key_id"])
    return record


# ── FastAPI dependency ────────────────────────────────────────────────────────

async def require_auth(x_atlas_key: str = Header(default=None)) -> dict:
    """
    FastAPI dependency. Inject into any route that requires authentication.
    Usage: async def my_route(auth: dict = Depends(require_auth)):
    """
    if not x_atlas_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-Atlas-Key header"
        )
    record = await validate_key(x_atlas_key)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key"
        )
    return record
