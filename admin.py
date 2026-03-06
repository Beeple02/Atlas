"""
Atlas Admin Panel
Full data browser at GET /admin — tables for every data type + CSV/JSON/Excel exports.
No authentication (internal network only). Do NOT expose Atlas publicly.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import database as db

logger = logging.getLogger(__name__)
admin_router = APIRouter(prefix="/admin")


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _ago(iso: str | None) -> str:
    if not iso:
        return "never"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        s = (datetime.now(timezone.utc) - dt).total_seconds()
        if s < 60:
            return f"{int(s)}s ago"
        elif s < 3600:
            return f"{int(s//60)}m ago"
        else:
            return f"{int(s//3600)}h ago"
    except Exception:
        return iso or "—"


# ── Export helpers ────────────────────────────────────────────────────────────

def _to_csv(rows: list[dict]) -> str:
    if not rows:
        return ""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=rows[0].keys())
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue()


def _to_excel(rows: list[dict], sheet_name: str = "Data") -> bytes:
    try:
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = sheet_name
        if not rows:
            buf = io.BytesIO()
            wb.save(buf)
            return buf.getvalue()
        headers = list(rows[0].keys())
        ws.append(headers)
        for row in rows:
            ws.append([row.get(h) for h in headers])
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()
    except ImportError:
        raise RuntimeError("openpyxl not installed")


# ── Export endpoints ──────────────────────────────────────────────────────────

async def _get_data(section: str, ticker: str | None) -> list[dict]:
    if section == "securities":
        return await db.get_all_securities()
    elif section == "orderbook":
        if ticker:
            ob = await db.get_orderbook(ticker.upper())
            return [ob] if ob else []
        return await db.get_all_orderbooks()
    elif section == "price_history":
        if ticker:
            return await db.get_price_history(ticker.upper(), days=365, limit=5000)
        # All tickers
        tickers = await db.get_all_tickers()
        all_rows = []
        for t in tickers:
            rows = await db.get_price_history(t, days=365, limit=5000)
            all_rows.extend(rows)
        return all_rows
    elif section == "ohlcv":
        if ticker:
            return await db.get_ohlcv(ticker.upper(), days=365)
        tickers = await db.get_all_tickers()
        all_rows = []
        for t in tickers:
            rows = await db.get_ohlcv(t, days=365)
            all_rows.extend(rows)
        return all_rows
    elif section == "derived":
        return await db.get_all_derived()
    elif section == "shareholders":
        if ticker:
            return await db.get_shareholders(ticker.upper())
        tickers = await db.get_all_tickers()
        all_rows = []
        for t in tickers:
            rows = await db.get_shareholders(t)
            all_rows.extend(rows)
        return all_rows
    elif section == "api_keys":
        return await db.list_api_keys()
    return []


@admin_router.get("/export/{section}")
async def export_data(
    section: str,
    fmt: str = Query(default="csv", description="csv | json | xlsx"),
    ticker: str | None = Query(default=None),
):
    rows = await _get_data(section, ticker)
    ts = _now_str()
    fname = f"atlas_{section}_{ts}"

    if fmt == "json":
        return JSONResponse(content=rows, headers={
            "Content-Disposition": f"attachment; filename={fname}.json"
        })
    elif fmt == "xlsx":
        try:
            data = _to_excel(rows, sheet_name=section)
            return StreamingResponse(
                io.BytesIO(data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={fname}.xlsx"}
            )
        except RuntimeError:
            return JSONResponse(status_code=500, content={"detail": "openpyxl not installed. Add it to requirements.txt."})
    else:  # csv default
        csv_data = _to_csv(rows)
        return StreamingResponse(
            io.StringIO(csv_data),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}.csv"}
        )


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _table(rows: list[dict], max_rows: int = 500) -> str:
    if not rows:
        return '<p style="color:#6b7280;padding:20px">No data.</p>'
    headers = list(rows[0].keys())
    truncated = len(rows) > max_rows
    display_rows = rows[:max_rows]

    th = "".join(f"<th>{h}</th>" for h in headers)
    trs = ""
    for i, row in enumerate(display_rows):
        bg = "#1e293b" if i % 2 == 0 else "#162032"
        tds = ""
        for h in headers:
            v = row.get(h)
            if v is None:
                val = '<span style="color:#475569">—</span>'
            elif isinstance(v, bool):
                val = f'<span style="color:{"#22c55e" if v else "#ef4444"}">{v}</span>'
            elif isinstance(v, (list, dict)):
                val = f'<span style="color:#94a3b8;font-size:11px">{str(v)[:80]}…</span>'
            else:
                val = str(v)
            tds += f"<td>{val}</td>"
        trs += f'<tr style="background:{bg}">{tds}</tr>'

    note = f'<p style="color:#f59e0b;font-size:12px;margin-top:8px">Showing first {max_rows} of {len(rows)} rows. Export to see all.</p>' if truncated else ""
    return f"""
    <div style="overflow-x:auto">
      <table>
        <thead><tr>{th}</tr></thead>
        <tbody>{trs}</tbody>
      </table>
    </div>
    {note}"""


def _export_bar(section: str, ticker_filter: str = "") -> str:
    tf = f"&ticker={ticker_filter}" if ticker_filter else ""
    return f"""
    <div class="export-bar">
      <span style="color:#64748b;font-size:13px">Export:</span>
      <a href="/admin/export/{section}?fmt=csv{tf}" class="btn-export">CSV</a>
      <a href="/admin/export/{section}?fmt=json{tf}" class="btn-export">JSON</a>
      <a href="/admin/export/{section}?fmt=xlsx{tf}" class="btn-export">Excel</a>
    </div>"""


def _section_html(section_id: str, title: str, rows: list[dict],
                  ticker_filter: str = "", extra: str = "") -> str:
    return f"""
    <div class="section" id="{section_id}">
      <div class="section-header">
        <h2>{title} <span class="count">{len(rows)} rows</span></h2>
        {_export_bar(section_id, ticker_filter)}
      </div>
      {extra}
      {_table(rows)}
    </div>"""


def _ticker_selector(tickers: list[str], current: str, section: str) -> str:
    opts = '<option value="">All tickers</option>'
    for t in tickers:
        sel = "selected" if t == current else ""
        opts += f'<option value="{t}" {sel}>{t}</option>'
    return f"""
    <div style="margin-bottom:12px">
      <label style="color:#94a3b8;font-size:13px;margin-right:8px">Filter by ticker:</label>
      <select onchange="window.location='/admin?section={section}&ticker='+this.value"
              style="background:#0f172a;color:#e2e8f0;border:1px solid #334155;padding:4px 8px;border-radius:4px;font-size:13px">
        {opts}
      </select>
    </div>"""


# ── Main admin page ───────────────────────────────────────────────────────────

@admin_router.get("", response_class=HTMLResponse, include_in_schema=False)
@admin_router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def admin_panel(
    section: str = Query(default="securities"),
    ticker: str = Query(default=""),
):
    tickers = await db.get_all_tickers()
    db_stats = await db.get_db_stats()
    initialized = await db.get_meta("atlas_initialized") == "true"
    ner_ok = await db.get_meta("ner_reachable") != "false"

    # ── Load data for active section ──
    content = ""
    if section == "securities":
        rows = await db.get_all_securities()
        content = _section_html("securities", "Securities & Prices", rows)

    elif section == "orderbook":
        extra = _ticker_selector(tickers, ticker, "orderbook")
        if ticker:
            ob = await db.get_orderbook(ticker.upper())
            rows = [ob] if ob else []
        else:
            rows = await db.get_all_orderbooks()
        content = _section_html("orderbook", "Orderbook Snapshots", rows, ticker, extra)

    elif section == "price_history":
        extra = _ticker_selector(tickers, ticker, "price_history")
        if ticker:
            rows = await db.get_price_history(ticker.upper(), days=365, limit=5000)
        else:
            rows = []
            for t in tickers:
                r = await db.get_price_history(t, days=365, limit=500)
                rows.extend(r)
        content = _section_html("price_history", "Price History", rows, ticker, extra)

    elif section == "ohlcv":
        extra = _ticker_selector(tickers, ticker, "ohlcv")
        if ticker:
            rows = await db.get_ohlcv(ticker.upper(), days=365)
        else:
            rows = []
            for t in tickers:
                r = await db.get_ohlcv(t, days=365)
                rows.extend(r)
        content = _section_html("ohlcv", "OHLCV Candles", rows, ticker, extra)

    elif section == "derived":
        rows = await db.get_all_derived()
        content = _section_html("derived", "Derived Metrics", rows)

    elif section == "shareholders":
        extra = _ticker_selector(tickers, ticker, "shareholders")
        if ticker:
            rows = await db.get_shareholders(ticker.upper())
        else:
            rows = []
            for t in tickers:
                r = await db.get_shareholders(t)
                rows.extend(r)
        content = _section_html("shareholders", "Shareholders", rows, ticker, extra)

    elif section == "api_keys":
        rows = await db.list_api_keys()
        content = _section_html("api_keys", "Atlas API Keys", rows)

    elif section == "meta":
        meta_keys = ["atlas_initialized", "ner_reachable", "last_poll_securities",
                     "last_poll_orderbook", "last_poll_price_history", "last_poll_ohlcv",
                     "last_poll_shareholders", "last_poll_stats", "ner_rate_limited_at"]
        rows = []
        for k in meta_keys:
            v = await db.get_meta(k)
            rows.append({"key": k, "value": v or "—"})
        content = _section_html("meta", "Atlas Metadata", rows)

    # ── Sidebar nav items ──
    nav_items = [
        ("securities",    "Securities & Prices",  db_stats.get("securities", 0)),
        ("orderbook",     "Orderbook",             db_stats.get("orderbook_history", 0)),
        ("price_history", "Price History",         db_stats.get("price_history", 0)),
        ("ohlcv",         "OHLCV",                 db_stats.get("ohlcv", 0)),
        ("derived",       "Derived Metrics",       db_stats.get("securities", 0)),
        ("shareholders",  "Shareholders",          db_stats.get("shareholders", 0)),
        ("api_keys",      "API Keys",              "—"),
        ("meta",          "Atlas Meta",            "—"),
    ]

    nav_html = ""
    for sid, slabel, scount in nav_items:
        active = "active" if sid == section else ""
        nav_html += f"""
        <a href="/admin?section={sid}" class="nav-item {active}">
          <span class="nav-label">{slabel}</span>
          <span class="nav-count">{scount}</span>
        </a>"""

    status_dot = f'<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{"#22c55e" if ner_ok else "#ef4444"};margin-right:6px"></span>'
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Atlas Admin</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
            background: #0f172a; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }}

    /* Header */
    header {{ background: #0d1b2a; border-bottom: 1px solid #1e3a5f;
              padding: 14px 24px; display: flex; align-items: center; justify-content: space-between; flex-shrink: 0; }}
    header h1 {{ font-size: 18px; font-weight: 700; color: #fff; letter-spacing: 2px; }}
    .header-right {{ font-size: 12px; color: #64748b; display: flex; align-items: center; gap: 16px; }}
    .status-pill {{ display: flex; align-items: center; }}

    /* Layout */
    .layout {{ display: flex; flex: 1; overflow: hidden; }}

    /* Sidebar */
    nav {{ width: 220px; background: #0d1b2a; border-right: 1px solid #1e3a5f;
           padding: 16px 0; flex-shrink: 0; overflow-y: auto; }}
    .nav-section-label {{ font-size: 10px; color: #475569; text-transform: uppercase;
                          letter-spacing: 1px; padding: 8px 16px 4px; }}
    .nav-item {{ display: flex; align-items: center; justify-content: space-between;
                 padding: 9px 16px; color: #94a3b8; text-decoration: none;
                 font-size: 13px; border-left: 3px solid transparent; transition: all 0.1s; }}
    .nav-item:hover {{ background: #1e293b; color: #e2e8f0; }}
    .nav-item.active {{ background: #1e3a5f; color: #93c5fd; border-left-color: #3b82f6; }}
    .nav-count {{ font-size: 11px; color: #475569; font-family: monospace; }}
    .nav-item.active .nav-count {{ color: #64748b; }}

    /* Main content */
    main {{ flex: 1; overflow-y: auto; padding: 24px; }}

    /* Section */
    .section {{ background: #1e293b; border: 1px solid #334155; border-radius: 8px;
                overflow: hidden; }}
    .section-header {{ display: flex; align-items: center; justify-content: space-between;
                       padding: 16px 20px; border-bottom: 1px solid #334155; flex-wrap: wrap; gap: 8px; }}
    .section-header h2 {{ font-size: 15px; font-weight: 600; color: #f1f5f9; }}
    .count {{ font-size: 12px; color: #64748b; font-weight: 400; margin-left: 8px; }}

    /* Export bar */
    .export-bar {{ display: flex; align-items: center; gap: 8px; }}
    .btn-export {{ background: #1e3a5f; color: #93c5fd; border: 1px solid #2563eb;
                   padding: 4px 12px; border-radius: 4px; font-size: 12px; font-weight: 600;
                   text-decoration: none; cursor: pointer; transition: background 0.1s; }}
    .btn-export:hover {{ background: #2563eb; color: #fff; }}

    /* Table */
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th {{ text-align: left; padding: 9px 12px; background: #0d1b2a; color: #93c5fd;
          font-weight: 600; font-size: 11px; text-transform: uppercase; letter-spacing: 0.5px;
          border-bottom: 1px solid #334155; white-space: nowrap; position: sticky; top: 0; }}
    td {{ padding: 8px 12px; border-bottom: 1px solid #1e3a5f; color: #cbd5e1;
          font-family: monospace; font-size: 12px; max-width: 300px;
          overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: #263348 !important; }}

    /* Ticker selector area */
    .filter-area {{ padding: 12px 20px; border-bottom: 1px solid #334155; background: #162032; }}
  </style>
</head>
<body>
  <header>
    <h1>⬡ ATLAS <span style="color:#334155;font-weight:400;letter-spacing:0">/ admin</span></h1>
    <div class="header-right">
      <span class="status-pill">{status_dot} NER {"OK" if ner_ok else "DOWN"}</span>
      <span>{"✓ Initialized" if initialized else "⏳ Initializing"}</span>
      <span>{now}</span>
      <a href="/dashboard" style="color:#3b82f6;text-decoration:none;font-size:12px">→ Dashboard</a>
    </div>
  </header>

  <div class="layout">
    <nav>
      <div class="nav-section-label">Data</div>
      {nav_html}
    </nav>
    <main>
      {content}
    </main>
  </div>
</body>
</html>"""

    return HTMLResponse(content=html)
