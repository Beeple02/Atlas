"""
Atlas Admin Panel
Data browser at /admin — light, minimal, clean.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import database as db
import ui

logger = logging.getLogger(__name__)
admin_router = APIRouter(prefix="/admin")

SECTIONS = [
    ("securities",    "Securities"),
    ("orderbook",     "Orderbook"),
    ("price_history", "Price History"),
    ("ohlcv",         "OHLCV"),
    ("derived",       "Derived Metrics"),
    ("shareholders",  "Shareholders"),
    ("api_keys",      "API Keys"),
    ("meta",          "Meta"),
]
FILTERABLE = {"orderbook", "price_history", "ohlcv", "shareholders"}


# ── Data fetcher ──────────────────────────────────────────────────────────────

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
        tickers = await db.get_all_tickers()
        rows = []
        for t in tickers:
            rows.extend(await db.get_price_history(t, days=365, limit=500))
        return rows
    elif section == "ohlcv":
        if ticker:
            return await db.get_ohlcv(ticker.upper(), days=365)
        tickers = await db.get_all_tickers()
        rows = []
        for t in tickers:
            rows.extend(await db.get_ohlcv(t, days=365))
        return rows
    elif section == "derived":
        return await db.get_all_derived()
    elif section == "shareholders":
        if ticker:
            return await db.get_shareholders(ticker.upper())
        tickers = await db.get_all_tickers()
        rows = []
        for t in tickers:
            rows.extend(await db.get_shareholders(t))
        return rows
    elif section == "api_keys":
        return await db.list_api_keys()
    elif section == "meta":
        keys = ["atlas_initialized", "ner_reachable", "last_poll_securities",
                "last_poll_orderbook", "last_poll_price_history", "last_poll_ohlcv",
                "last_poll_shareholders", "last_poll_stats", "ner_rate_limited_at"]
        rows = []
        for k in keys:
            v = await db.get_meta(k)
            rows.append({"key": k, "value": v or ""})
        return rows
    return []


# ── Export ────────────────────────────────────────────────────────────────────

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
        if rows:
            headers = list(rows[0].keys())
            ws.append(headers)
            for row in rows:
                ws.append([row.get(h) for h in headers])
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()
    except ImportError:
        raise RuntimeError("openpyxl not installed")


@admin_router.get("/export/{section}")
async def export_data(
    section: str,
    fmt: str = Query(default="csv"),
    ticker: str | None = Query(default=None),
):
    rows = await _get_data(section, ticker)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fname = f"atlas_{section}_{ts}"
    if fmt == "json":
        return JSONResponse(content=rows, headers={
            "Content-Disposition": f"attachment; filename={fname}.json"
        })
    elif fmt == "xlsx":
        try:
            data = _to_excel(rows, sheet_name=section)
            return StreamingResponse(io.BytesIO(data),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename={fname}.xlsx"})
        except RuntimeError as e:
            return JSONResponse(status_code=500, content={"detail": str(e)})
    else:
        return StreamingResponse(io.StringIO(_to_csv(rows)),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}.csv"})


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _table(rows: list[dict], max_rows: int = 500) -> str:
    if not rows:
        return '<div class="empty-state">No data available.</div>'
    headers = list(rows[0].keys())
    truncated = len(rows) > max_rows
    th = "".join(f"<th>{h}</th>" for h in headers)
    trs = ""
    for row in rows[:max_rows]:
        tds = ""
        for h in headers:
            v = row.get(h)
            if v is None:
                tds += '<td style="color:#d4d4d4">—</td>'
            elif isinstance(v, (list, dict)):
                tds += f"<td title='{json.dumps(v)}'>[object]</td>"
            else:
                tds += f"<td>{v}</td>"
        trs += f"<tr>{tds}</tr>"
    note = f'<p class="trunc-note">Showing {max_rows} of {len(rows)} rows. Export to see all.</p>' if truncated else ""
    return f'<div class="tbl-wrap"><table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>{note}'


# ── Main route ────────────────────────────────────────────────────────────────

@admin_router.get("", response_class=HTMLResponse, include_in_schema=False)
@admin_router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def admin_panel(
    section: str = Query(default="securities"),
    ticker: str = Query(default=""),
):
    tickers = await db.get_all_tickers()
    ner_ok = await db.get_meta("ner_reachable") != "false"
    initialized = await db.get_meta("atlas_initialized") == "true"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rows = await _get_data(section, ticker or None)

    # Topbar right side
    ner_pill = f'<span class="tb-pill {"ok" if ner_ok else "err"}"><span class="dot"></span>NER</span>'
    init_pill = f'<span class="tb-pill {"ok" if initialized else "warn"}"><span class="dot"></span>{"Ready" if initialized else "Init…"}</span>'
    right = f'{ner_pill}{init_pill}<span>{now}</span><a href="/dashboard">Dashboard</a><a href="/docs">API Docs</a>'
    tb = ui.topbar("Atlas", "Admin", right)

    # Nav tabs
    nav_tabs = "".join(
        f'<a href="/admin?section={sid}" class="nav-tab{"  active" if sid == section else ""}">{label}</a>'
        for sid, label in SECTIONS
    )
    nav = f'<div id="nav">{nav_tabs}</div>'

    # Toolbar
    tf = f"&ticker={ticker}" if ticker else ""
    filter_html = ""
    if section in FILTERABLE:
        opts = '<option value="">All tickers</option>' + "".join(
            f'<option value="{t}"{"selected" if t == ticker else ""}>{t}</option>'
            for t in tickers
        )
        filter_html = f'''<span class="tb-label">Ticker</span>
        <select onchange="location='/admin?section={section}&ticker='+this.value">{opts}</select>'''

    toolbar = f'''<div id="toolbar">
      {filter_html}
      <div class="export-group">
        <span class="lbl">Export</span>
        <a href="/admin/export/{section}?fmt=csv{tf}" class="btn-export">CSV</a>
        <a href="/admin/export/{section}?fmt=json{tf}" class="btn-export">JSON</a>
        <a href="/admin/export/{section}?fmt=xlsx{tf}" class="btn-export">Excel</a>
      </div>
      <span class="row-count">{len(rows):,} rows</span>
    </div>'''

    # Section label + table
    section_label = dict(SECTIONS).get(section, section)
    content = f'''<div id="content">
      <div style="margin-bottom:16px">
        <h1 style="font-size:16px;font-weight:600;color:#111">{section_label}</h1>
        <p style="font-size:12px;color:#a3a3a3;margin-top:2px">{len(rows):,} records</p>
      </div>
      {_table(rows)}
    </div>'''

    return HTMLResponse(content=ui.page(
        title=f"Atlas / {section_label}",
        topbar=tb, nav=nav, toolbar=toolbar, content=content
    ))
