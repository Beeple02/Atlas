"""
Atlas Admin Panel
Data browser at /admin — light, minimal, clean.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query, Request, Form
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, RedirectResponse

import database as db
import ui

logger = logging.getLogger(__name__)
admin_router = APIRouter(prefix="/admin")

SECTIONS = [
    ("securities",              "Securities"),
    ("orderbook",               "Orderbook"),
    ("orderbook_history",       "OB History"),
    ("price_history",           "Price History"),
    ("ohlcv",                   "OHLCV"),
    ("derived",                 "Derived Metrics"),
    ("shareholders",            "Shareholders"),
    ("options_contracts",       "Options"),
    ("bonds",                   "Bonds"),
    ("bond_price_history",      "Bond History"),
    ("prediction_contracts",    "Predictions"),
    ("api_keys",                "API Keys"),
    ("meta",                    "Meta"),
]
FILTERABLE = {"orderbook", "orderbook_history", "price_history", "ohlcv", "shareholders",
              "options_contracts", "bonds", "bond_price_history"}


# ── Data fetcher ──────────────────────────────────────────────────────────────

async def _get_data(section: str, ticker: str | None) -> list[dict]:
    if section == "securities":
        return await db.get_all_securities()
    elif section == "orderbook":
        if ticker:
            ob = await db.get_orderbook(ticker.upper())
            return [ob] if ob else []
        return await db.get_all_orderbooks()
    elif section == "orderbook_history":
        return await db.get_orderbook_history(ticker or None, limit=1000)
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
                "last_poll_shareholders", "last_poll_stats", "ner_rate_limited_at",
                "tse_last_securities_poll", "tse_last_price_poll", "tse_last_ohlcv_poll",
                "tse_last_options_poll", "tse_last_bonds_poll", "tse_last_contracts_poll"]
        rows = []
        for k in keys:
            v = await db.get_meta(k)
            rows.append({"key": k, "value": v or ""})
        return rows
    elif section == "options_contracts":
        include_expired = ticker == "__all__"
        symbol_filter = ticker if ticker and ticker != "__all__" else None
        return await db.get_all_options_contracts(active_only=not include_expired, symbol=symbol_filter)
    elif section == "bonds":
        include_matured = ticker == "__all__"
        return await db.get_all_bonds(active_only=not include_matured)
    elif section == "bond_price_history":
        if ticker:
            bond = await db.get_bond_by_symbol(ticker.upper())
            if bond:
                return await db.get_bond_price_history(bond["bond_id"], days=365, limit=5000)
        import aiosqlite
        from config import settings as cfg
        async with aiosqlite.connect(cfg.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute("SELECT * FROM bond_price_history ORDER BY timestamp DESC LIMIT 2000")
            return [dict(r) for r in await cur.fetchall()]
    elif section == "prediction_contracts":
        return await db.get_all_prediction_contracts(active_only=False)
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

@admin_router.post("/keys/create", response_class=HTMLResponse, include_in_schema=False)
async def create_key(tool_id: str = Form(...), tool_name: str = Form(...)):
    from auth import create_tool_key
    try:
        plaintext_key = await create_tool_key(tool_id.strip(), tool_name.strip())
    except Exception as e:
        return HTMLResponse(content=_key_error_page(str(e)), status_code=400)
    return HTMLResponse(content=_new_key_page(tool_id, tool_name, plaintext_key))


@admin_router.post("/keys/revoke", include_in_schema=False)
async def revoke_key(tool_id: str = Form(...)):
    await db.deactivate_api_key(tool_id)
    return RedirectResponse(url="/admin?section=api_keys", status_code=303)


def _new_key_page(tool_id: str, tool_name: str, key: str) -> str:
    tb = ui.topbar("Atlas", "Admin / New Key", '<a href="/admin?section=api_keys">← Back to API Keys</a>')
    content = f"""<div id="content" style="max-width:600px">
      <div style="border:1px solid #bbf7d0;border-radius:8px;padding:24px;background:#f0fdf4;margin-bottom:20px">
        <p style="font-size:13px;font-weight:600;color:#15803d;margin-bottom:4px">✓ Key created for {tool_name}</p>
        <p style="font-size:12px;color:#166534">Copy this key now — it will never be shown again.</p>
      </div>
      <div style="border:1px solid #e5e5e5;border-radius:8px;padding:20px">
        <p style="font-size:11px;color:#a3a3a3;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:8px">API Key</p>
        <div style="display:flex;align-items:center;gap:10px">
          <code id="keyval" style="font-family:'Geist Mono',monospace;font-size:13px;background:#f5f5f5;
            padding:10px 14px;border-radius:6px;border:1px solid #e5e5e5;flex:1;word-break:break-all">{key}</code>
          <button onclick="navigator.clipboard.writeText('{key}');this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',2000)"
            style="font-family:inherit;font-size:12px;font-weight:500;background:#111;color:#fff;border:none;
              border-radius:6px;padding:8px 14px;cursor:pointer;white-space:nowrap">Copy</button>
        </div>
        <div style="margin-top:16px;padding-top:16px;border-top:1px solid #e5e5e5">
          <p style="font-size:12px;color:#737373;margin-bottom:6px">Use it in requests:</p>
          <code style="font-family:'Geist Mono',monospace;font-size:12px;color:#374151">
            X-Atlas-Key: {key}
          </code>
        </div>
      </div>
      <p style="margin-top:16px;font-size:12px;color:#a3a3a3">
        Tool ID: <code style="font-family:'Geist Mono',monospace">{tool_id}</code>
      </p>
      <a href="/admin?section=api_keys" style="display:inline-block;margin-top:20px;font-size:13px;color:#111;
        text-decoration:none;border:1px solid #e5e5e5;border-radius:6px;padding:8px 16px">
        ← Back to API Keys
      </a>
    </div>"""
    return ui.page("Atlas / New Key", tb, "", "", content)


def _key_error_page(error: str) -> str:
    tb = ui.topbar("Atlas", "Admin / Error", '<a href="/admin?section=api_keys">← Back</a>')
    content = f"""<div id="content" style="max-width:600px">
      <div style="border:1px solid #fecaca;border-radius:8px;padding:20px;background:#fef2f2">
        <p style="font-size:13px;font-weight:600;color:#dc2626">Failed to create key</p>
        <p style="font-size:12px;color:#991b1b;margin-top:4px">{error}</p>
      </div>
      <a href="/admin?section=api_keys" style="display:inline-block;margin-top:16px;font-size:13px;color:#111;
        text-decoration:none;border:1px solid #e5e5e5;border-radius:6px;padding:8px 16px">← Back</a>
    </div>"""
    return ui.page("Atlas / Error", tb, "", "", content)



@admin_router.get("/keys/{tool_id}", response_class=HTMLResponse, include_in_schema=False)
async def key_detail(tool_id: str):
    keys = await db.list_api_keys()
    key_record = next((k for k in keys if k["key_id"] == tool_id), None)
    if not key_record:
        return HTMLResponse(content=_key_error_page(f"Key '{tool_id}' not found."), status_code=404)

    stats = await db.get_key_stats(tool_id)
    tb = ui.topbar("Atlas", f"Admin / API Keys / {tool_id}", '<a href="/admin?section=api_keys">← API Keys</a>')
    active = key_record.get("active")
    status_badge = f'<span class="badge {"green" if active else "red"}">{"Active" if active else "Revoked"}</span>'

    stat_cards = f"""<div class="stat-grid" style="margin-bottom:24px">
      <div class="stat-card"><div class="s-label">Status</div>
        <div class="s-value" style="font-size:14px;margin-top:4px">{status_badge}</div></div>
      <div class="stat-card"><div class="s-label">Total Requests</div>
        <div class="s-value">{stats["total"]:,}</div></div>
      <div class="stat-card"><div class="s-label">Last 24h</div>
        <div class="s-value">{stats["last_24h"]:,}</div></div>
      <div class="stat-card"><div class="s-label">Last 7 days</div>
        <div class="s-value">{stats["last_7d"]:,}</div></div>
      <div class="stat-card"><div class="s-label">Created</div>
        <div class="s-value" style="font-size:13px">{(key_record.get("created_at") or "—")[:10]}</div></div>
      <div class="stat-card"><div class="s-label">Last Used</div>
        <div class="s-value" style="font-size:13px">{(key_record.get("last_used") or "never")[:16]}</div></div>
    </div>"""

    revoke_form = f"""<form method="post" action="/admin/keys/revoke"
        style="margin-top:16px;padding-top:16px;border-top:1px solid #e5e5e5">
      <input type="hidden" name="tool_id" value="{tool_id}">
      <button type="submit" onclick="return confirm('Revoke this key?')"
        style="font-family:inherit;font-size:12px;font-weight:500;color:#dc2626;background:#fff;
          border:1px solid #fecaca;border-radius:6px;padding:6px 14px;cursor:pointer">
        Revoke Key
      </button>
    </form>""" if active else ""

    header_block = f"""<div style="border:1px solid #e5e5e5;border-radius:8px;padding:20px;margin-bottom:24px">
      <p style="font-size:11px;color:#a3a3a3;text-transform:uppercase;letter-spacing:0.06em;margin-bottom:12px">Usage</p>
      <div style="background:#f5f5f5;border-radius:6px;padding:10px 14px;font-family:'Geist Mono',monospace;font-size:12px;color:#374151;margin-bottom:8px">
        X-Atlas-Key: atl_{tool_id}_<span style="color:#a3a3a3">&lt;secret&gt;</span>
      </div>
      <p style="font-size:12px;color:#a3a3a3">The full plaintext key was shown once at creation. If lost, revoke and create a new one.</p>
      {revoke_form}
    </div>"""

    if stats["top_endpoints"]:
        ep_rows = "".join(
            f"<tr><td style='font-family:inherit'>{r['endpoint']}</td><td>{r['n']:,}</td></tr>"
            for r in stats["top_endpoints"]
        )
        endpoints_block = f"""<div style="border:1px solid #e5e5e5;border-radius:8px;overflow:hidden;margin-bottom:24px">
          <div style="padding:14px 20px;border-bottom:1px solid #e5e5e5">
            <h2 style="font-size:13px;font-weight:600">Top Endpoints</h2></div>
          <div style="padding:0 20px 4px">
            <table><thead><tr><th>Endpoint</th><th>Requests</th></tr></thead>
            <tbody>{ep_rows}</tbody></table></div>
        </div>"""
    else:
        endpoints_block = ""

    if stats["recent"]:
        recent_rows = "".join(
            f"<tr><td style='font-family:inherit'>{r['method']}</td><td>{r['endpoint']}</td><td>{r['status_code']}</td><td>{r['ts'][:19]}</td></tr>"
            for r in stats["recent"]
        )
        recent_block = f"""<div style="border:1px solid #e5e5e5;border-radius:8px;overflow:hidden">
          <div style="padding:14px 20px;border-bottom:1px solid #e5e5e5">
            <h2 style="font-size:13px;font-weight:600">Recent Requests
              <span style="color:#a3a3a3;font-weight:400;font-size:12px">(last 50)</span></h2></div>
          <div style="padding:0 20px 4px">
            <table><thead><tr><th>Method</th><th>Endpoint</th><th>Status</th><th>Time</th></tr></thead>
            <tbody>{recent_rows}</tbody></table></div>
        </div>"""
    else:
        recent_block = '<div style="border:1px solid #e5e5e5;border-radius:8px;padding:32px;text-align:center;color:#a3a3a3;font-size:13px">No requests logged yet.</div>'

    content = f"""<div id="content">
      <div style="margin-bottom:20px;display:flex;align-items:baseline;gap:12px">
        <h1 style="font-size:16px;font-weight:600">{key_record.get("tool_name", tool_id)}</h1>
        <code style="font-size:12px;color:#a3a3a3;font-family:'Geist Mono',monospace">{tool_id}</code>
      </div>
      {stat_cards}{header_block}{endpoints_block}{recent_block}
    </div>"""

    return HTMLResponse(content=ui.page(f"Atlas / {tool_id}", tb, "", "", content))


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

    # Special UI for api_keys section
    extra_html = ""
    if section == "api_keys":
        new_key_banner = ""
        new_key = request_new_key if hasattr(locals(), "request_new_key") else None
        extra_html = f"""
        <div style="border:1px solid #e5e5e5;border-radius:8px;padding:20px;margin-bottom:20px;background:#fafafa">
          <h2 style="font-size:13px;font-weight:600;margin-bottom:12px">Create New Key</h2>
          <form method="post" action="/admin/keys/create" style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
            <div>
              <label style="font-size:11px;color:#737373;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.05em">Tool ID</label>
              <input name="tool_id" required placeholder="e.g. bloomberg_terminal"
                style="font-family:inherit;font-size:13px;border:1px solid #d4d4d4;border-radius:6px;padding:6px 10px;width:200px;outline:none">
            </div>
            <div>
              <label style="font-size:11px;color:#737373;display:block;margin-bottom:4px;text-transform:uppercase;letter-spacing:0.05em">Display Name</label>
              <input name="tool_name" required placeholder="e.g. Bloomberg Terminal"
                style="font-family:inherit;font-size:13px;border:1px solid #d4d4d4;border-radius:6px;padding:6px 10px;width:220px;outline:none">
            </div>
            <button type="submit"
              style="font-family:inherit;font-size:13px;font-weight:500;background:#111;color:#fff;border:none;border-radius:6px;padding:7px 16px;cursor:pointer">
              Generate Key
            </button>
          </form>
        </div>"""

        # Table with revoke buttons
        if rows:
            headers = list(rows[0].keys())
            th = "".join(f"<th>{h}</th>" for h in headers) + "<th>Actions</th>"
            trs = ""
            for row in rows:
                tds = ""
                for h in headers:
                    v = row.get(h)
                    if h == "active":
                        tds += f'<td style="font-family:inherit"><span class="badge {"green" if v else "red"}">{"Active" if v else "Revoked"}</span></td>'
                    elif h == "key_id":
                        tds += f'<td style="font-family:inherit"><a href="/admin/keys/{v}" style="color:#111;font-weight:500;text-decoration:underline;text-underline-offset:2px">{v}</a></td>'
                    elif v is None:
                        tds += '<td style="color:#d4d4d4">—</td>'
                    else:
                        tds += f"<td>{v}</td>"
                # Revoke button only for active keys
                if row.get("active"):
                    revoke = f'''<td style="font-family:inherit">
                      <form method="post" action="/admin/keys/revoke" style="display:inline">
                        <input type="hidden" name="tool_id" value="{row["key_id"]}">
                        <button type="submit" onclick="return confirm('Revoke key for {row.get("tool_name","this tool")}?')"
                          style="font-family:inherit;font-size:11px;color:#dc2626;background:none;border:1px solid #fecaca;border-radius:4px;padding:2px 8px;cursor:pointer">
                          Revoke
                        </button>
                      </form>
                    </td>'''
                else:
                    revoke = "<td>—</td>"
                trs += f"<tr>{tds}{revoke}</tr>"
            key_table = f'<div class="tbl-wrap"><table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>'
        else:
            key_table = '<div class="empty-state">No API keys yet. Create one above.</div>'

        content = f'''<div id="content">
          <div style="margin-bottom:16px">
            <h1 style="font-size:16px;font-weight:600;color:#111">{section_label}</h1>
            <p style="font-size:12px;color:#a3a3a3;margin-top:2px">{len(rows):,} keys</p>
          </div>
          {extra_html}
          {key_table}
        </div>'''
    else:
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
