"""
Atlas Dashboard
Status overview at /dashboard — light, minimal, clean.
Auto-refreshes every 30s.
"""

from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import database as db
import ui

dashboard_router = APIRouter()


def _ago(iso: str | None) -> str:
    if not iso:
        return "never"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        s = (datetime.now(timezone.utc) - dt).total_seconds()
        if s < 60:    return f"{int(s)}s ago"
        elif s < 3600: return f"{int(s//60)}m ago"
        else:          return f"{int(s//3600)}h ago"
    except Exception:
        return iso or "—"


@dashboard_router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    securities  = await db.get_all_securities()
    derived_list = await db.get_all_derived()
    derived_map  = {d["ticker"]: d for d in derived_list}
    db_stats    = await db.get_db_stats()
    initialized = await db.get_meta("atlas_initialized") == "true"
    ner_ok      = await db.get_meta("ner_reachable") != "false"
    now         = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    poll_keys = ["securities", "orderbook", "price_history", "ohlcv", "shareholders", "stats"]
    last_polls = {k: await db.get_meta(f"last_poll_{k}") for k in poll_keys}

    # ── Stat cards ──
    active_count = sum(1 for s in securities if not s.get("frozen"))
    stats_html = f"""<div class="stat-grid">
      <div class="stat-card">
        <div class="s-label">Status</div>
        <div class="s-value" style="font-size:14px;margin-top:4px">
          <span class="badge {"green" if initialized else "gray"}">{"Ready" if initialized else "Initializing"}</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="s-label">NER API</div>
        <div class="s-value" style="font-size:14px;margin-top:4px">
          <span class="badge {"green" if ner_ok else "red"}">{"Reachable" if ner_ok else "Unreachable"}</span>
        </div>
      </div>
      <div class="stat-card">
        <div class="s-label">Securities</div>
        <div class="s-value">{len(securities)}</div>
        <div class="s-sub">{active_count} active</div>
      </div>
      <div class="stat-card">
        <div class="s-label">Price History</div>
        <div class="s-value">{db_stats.get("price_history", 0):,}</div>
        <div class="s-sub">rows</div>
      </div>
      <div class="stat-card">
        <div class="s-label">OHLCV Candles</div>
        <div class="s-value">{db_stats.get("ohlcv", 0):,}</div>
      </div>
      <div class="stat-card">
        <div class="s-label">OB History</div>
        <div class="s-value">{db_stats.get("orderbook_history", 0):,}</div>
        <div class="s-sub">rows</div>
      </div>
    </div>"""

    # ── Market table ──
    sec_rows = ""
    for sec in securities:
        d = derived_map.get(sec["ticker"], {})
        frozen = sec.get("frozen", False)
        status_badge = f'<span class="badge {"red" if frozen else "green"}">{"Frozen" if frozen else "Active"}</span>'

        imb = d.get("orderbook_imbalance")
        if imb is not None:
            imb_color = "#16a34a" if imb > 0.1 else ("#dc2626" if imb < -0.1 else "#737373")
            imb_str = f'<span style="color:{imb_color}">{imb:.3f}</span>'
        else:
            imb_str = '<span style="color:#d4d4d4">—</span>'

        liq = d.get("liquidity_score")
        liq_str = f"{liq:.1f}" if liq is not None else "—"

        sec_rows += f"""<tr>
          <td style="font-family:inherit;font-weight:600">{sec["ticker"]}</td>
          <td style="font-family:inherit;color:#374151">{sec.get("full_name","—")}</td>
          <td>{sec.get("market_price","—")}</td>
          <td>{d.get("vwap_24h") or "—"}</td>
          <td>{d.get("vwap_7d") or "—"}</td>
          <td>{f'{d["spread_pct"]:.3f}%' if d.get("spread_pct") else "—"}</td>
          <td>{imb_str}</td>
          <td>{liq_str}</td>
          <td style="font-family:inherit">{status_badge}</td>
        </tr>"""

    market_block = f"""<div class="section-block">
      <div class="section-block-header">
        <h2>Live Market Data</h2>
        <a href="/admin" style="font-size:12px;color:#737373;text-decoration:none">View all data →</a>
      </div>
      <div class="section-block-body">
        <table>
          <thead><tr>
            <th>Ticker</th><th>Name</th><th>Price</th>
            <th>VWAP 24h</th><th>VWAP 7d</th><th>Spread %</th>
            <th>OB Imbalance</th><th>Liquidity</th><th>Status</th>
          </tr></thead>
          <tbody>{sec_rows if sec_rows else
            '<tr><td colspan="9" style="text-align:center;color:#a3a3a3;padding:32px 0">No data yet</td></tr>'
          }</tbody>
        </table>
      </div>
    </div>"""

    # ── Poll status ──
    poll_items = ""
    labels = {"securities": "Securities", "orderbook": "Orderbook",
              "price_history": "Price History", "ohlcv": "OHLCV",
              "shareholders": "Shareholders", "stats": "Financial Stats"}
    for k in poll_keys:
        poll_items += f"""<div class="poll-item">
          <div class="p-label">{labels[k]}</div>
          <div class="p-value">{_ago(last_polls[k])}</div>
        </div>"""

    poll_block = f"""<div class="section-block">
      <div class="section-block-header"><h2>Last Poll</h2></div>
      <div class="section-block-body" style="padding:16px 20px">
        <div class="poll-grid">{poll_items}</div>
      </div>
    </div>"""

    # ── Assemble ──
    ner_pill = f'<span class="tb-pill {"ok" if ner_ok else "err"}"><span class="dot"></span>NER</span>'
    init_pill = f'<span class="tb-pill {"ok" if initialized else "warn"}"><span class="dot"></span>{"Ready" if initialized else "Init…"}</span>'
    right = f'{ner_pill}{init_pill}<span>Refreshes every 30s</span><span>{now}</span><a href="/admin">Admin</a><a href="/docs">API Docs</a>'

    tb  = ui.topbar("Atlas", "Dashboard", right)
    nav = ""  # dashboard has no sub-nav
    toolbar = ""

    content = f'<div id="content">{stats_html}{market_block}{poll_block}</div>'

    return HTMLResponse(content=ui.page(
        title="Atlas — Dashboard",
        topbar=tb, nav=nav, toolbar=toolbar, content=content,
        refresh=30
    ))
