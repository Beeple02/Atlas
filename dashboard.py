"""
Atlas Dashboard
A simple HTML status page served at GET /dashboard.
No authentication required — intended for operator monitoring only.
DO NOT expose Atlas to the public internet.
"""

from datetime import datetime, timezone
from fastapi import APIRouter
from fastapi.responses import HTMLResponse

import database as db

dashboard_router = APIRouter()


def _ago(iso: str | None) -> str:
    if not iso:
        return "never"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        seconds = (datetime.now(timezone.utc) - dt).total_seconds()
        if seconds < 60:
            return f"{int(seconds)}s ago"
        elif seconds < 3600:
            return f"{int(seconds // 60)}m ago"
        else:
            return f"{int(seconds // 3600)}h ago"
    except Exception:
        return iso


def _status_dot(value: bool) -> str:
    color = "#22c55e" if value else "#ef4444"
    return f'<span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:{color};margin-right:6px;"></span>'


@dashboard_router.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    securities = await db.get_all_securities()
    derived_list = await db.get_all_derived()
    derived_map = {d["ticker"]: d for d in derived_list}
    db_stats = await db.get_db_stats()
    initialized = await db.get_meta("atlas_initialized") == "true"
    ner_ok = await db.get_meta("ner_reachable") != "false"

    last_polls = {}
    for key in ["securities", "orderbook", "price_history", "ohlcv", "shareholders", "stats"]:
        last_polls[key] = await db.get_meta(f"last_poll_{key}")

    # Build securities rows
    sec_rows = ""
    for sec in securities:
        d = derived_map.get(sec["ticker"], {})
        frozen_badge = '<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:4px;font-size:12px;">FROZEN</span>' if sec["frozen"] else '<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:4px;font-size:12px;">ACTIVE</span>'
        imbalance = d.get("orderbook_imbalance")
        imb_color = "#22c55e" if imbalance and imbalance > 0.1 else ("#ef4444" if imbalance and imbalance < -0.1 else "#94a3b8")
        imb_str = f'<span style="color:{imb_color}">{imbalance:.3f}</span>' if imbalance is not None else "—"
        sec_rows += f"""
        <tr>
          <td style="font-weight:600;color:#1a56db">{sec["ticker"]}</td>
          <td>{sec.get("full_name","—")}</td>
          <td style="font-family:monospace">{sec.get("market_price","—")}</td>
          <td style="font-family:monospace">{d.get("vwap_24h","—") or "—"}</td>
          <td style="font-family:monospace">{d.get("vwap_7d","—") or "—"}</td>
          <td style="font-family:monospace">{d.get("spread_pct","—") or "—"}{"%" if d.get("spread_pct") else ""}</td>
          <td>{imb_str}</td>
          <td style="font-family:monospace">{d.get("liquidity_score","—") or "—"}</td>
          <td>{frozen_badge}</td>
        </tr>"""

    # Poll status rows
    poll_rows = ""
    for key, label in [
        ("securities", "Securities list"),
        ("orderbook", "Orderbook"),
        ("price_history", "Price history"),
        ("ohlcv", "OHLCV"),
        ("shareholders", "Shareholders"),
        ("stats", "Financial stats"),
    ]:
        ago = _ago(last_polls.get(key))
        poll_rows += f"<tr><td>{label}</td><td style='font-family:monospace;color:#64748b'>{ago}</td></tr>"

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Atlas — Bloomberg Labs</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f172a; color: #e2e8f0; min-height: 100vh; }}
    header {{ background: #0d1b2a; border-bottom: 1px solid #1e3a5f; padding: 20px 40px; display: flex; align-items: center; justify-content: space-between; }}
    header h1 {{ font-size: 24px; font-weight: 700; color: #fff; letter-spacing: 2px; }}
    header span {{ font-size: 13px; color: #64748b; }}
    .main {{ padding: 32px 40px; max-width: 1400px; margin: 0 auto; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 32px; }}
    .card {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 20px; }}
    .card .label {{ font-size: 12px; color: #64748b; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 8px; }}
    .card .value {{ font-size: 28px; font-weight: 700; color: #f1f5f9; }}
    .card .sub {{ font-size: 13px; color: #94a3b8; margin-top: 4px; }}
    .section {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 24px; margin-bottom: 24px; }}
    .section h2 {{ font-size: 16px; font-weight: 600; color: #93c5fd; margin-bottom: 16px; text-transform: uppercase; letter-spacing: 1px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    th {{ text-align: left; padding: 10px 12px; background: #0d1b2a; color: #93c5fd; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px; border-bottom: 1px solid #334155; }}
    td {{ padding: 10px 12px; border-bottom: 1px solid #1e3a5f; color: #cbd5e1; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #263348; }}
    .status-ok {{ color: #22c55e; font-weight: 600; }}
    .status-err {{ color: #ef4444; font-weight: 600; }}
    .badge-init {{ display: inline-block; background: #dcfce7; color: #166534; padding: 4px 12px; border-radius: 20px; font-size: 13px; font-weight: 600; }}
    .badge-noinit {{ display: inline-block; background: #fef3c7; color: #92400e; padding: 4px 12px; border-radius: 20px; font-size: 13px; font-weight: 600; }}
    .refresh-note {{ font-size: 12px; color: #475569; text-align: right; margin-top: 8px; }}
  </style>
</head>
<body>
  <header>
    <h1>⬡ ATLAS</h1>
    <span>Bloomberg Labs — Market Data Infrastructure &nbsp;·&nbsp; Auto-refreshes every 30s &nbsp;·&nbsp; {now}</span>
  </header>

  <div class="main">
    <!-- Status cards -->
    <div class="cards">
      <div class="card">
        <div class="label">Status</div>
        <div class="value" style="font-size:18px;margin-top:4px">
          {"<span class='badge-init'>✓ Initialized</span>" if initialized else "<span class='badge-noinit'>⏳ Initializing</span>"}
        </div>
      </div>
      <div class="card">
        <div class="label">NER API</div>
        <div class="value" style="font-size:18px;margin-top:4px">
          {_status_dot(ner_ok)}{"<span class='status-ok'>Reachable</span>" if ner_ok else "<span class='status-err'>Unreachable</span>"}
        </div>
      </div>
      <div class="card">
        <div class="label">Tracked Securities</div>
        <div class="value">{len(securities)}</div>
        <div class="sub">Active: {sum(1 for s in securities if not s["frozen"])}</div>
      </div>
      <div class="card">
        <div class="label">Price History Rows</div>
        <div class="value">{db_stats.get("price_history", 0):,}</div>
      </div>
      <div class="card">
        <div class="label">OHLCV Candles</div>
        <div class="value">{db_stats.get("ohlcv", 0):,}</div>
      </div>
      <div class="card">
        <div class="label">OB History Rows</div>
        <div class="value">{db_stats.get("orderbook_history", 0):,}</div>
      </div>
    </div>

    <!-- Market data table -->
    <div class="section">
      <h2>Live Market Data</h2>
      <table>
        <thead>
          <tr>
            <th>Ticker</th><th>Name</th><th>Price</th><th>VWAP 24h</th>
            <th>VWAP 7d</th><th>Spread %</th><th>OB Imbalance</th><th>Liquidity</th><th>Status</th>
          </tr>
        </thead>
        <tbody>{sec_rows if sec_rows else "<tr><td colspan='9' style='text-align:center;color:#475569;padding:32px'>No securities loaded yet</td></tr>"}</tbody>
      </table>
    </div>

    <!-- Poll status -->
    <div class="section">
      <h2>Last Successful Poll</h2>
      <table>
        <thead><tr><th>Data Source</th><th>Last Updated</th></tr></thead>
        <tbody>{poll_rows}</tbody>
      </table>
    </div>

  </div>
</body>
</html>"""

    return HTMLResponse(content=html)
