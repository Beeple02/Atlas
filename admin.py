"""
Atlas Admin Panel — Brutalist Edition
Raw data browser. No fluff.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Query
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

import database as db

logger = logging.getLogger(__name__)
admin_router = APIRouter(prefix="/admin")


def _now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


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


@admin_router.get("/export/{section}")
async def export_data(
    section: str,
    fmt: str = Query(default="csv"),
    ticker: str | None = Query(default=None),
):
    rows = await _get_data(section, ticker)
    fname = f"atlas_{section}_{_now_str()}"

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
        except RuntimeError as e:
            return JSONResponse(status_code=500, content={"detail": str(e)})
    else:
        return StreamingResponse(
            io.StringIO(_to_csv(rows)),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}.csv"}
        )


# ── HTML ──────────────────────────────────────────────────────────────────────

def _table(rows: list[dict], max_rows: int = 500) -> str:
    if not rows:
        return '<p class="empty">— no data —</p>'
    headers = list(rows[0].keys())
    truncated = len(rows) > max_rows
    th = "".join(f"<th>{h}</th>" for h in headers)
    trs = ""
    for row in rows[:max_rows]:
        tds = ""
        for h in headers:
            v = row.get(h)
            if v is None:
                tds += "<td>—</td>"
            elif isinstance(v, (list, dict)):
                tds += f"<td title='{json.dumps(v)}'>[…]</td>"
            else:
                tds += f"<td>{v}</td>"
        trs += f"<tr>{tds}</tr>"
    note = f'<p class="truncnote">showing {max_rows}/{len(rows)} rows — export to see all</p>' if truncated else ""
    return f'<div class="tbl-wrap"><table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table></div>{note}'


def _ticker_sel(tickers: list[str], current: str, section: str) -> str:
    opts = '<option value="">all</option>'
    for t in tickers:
        sel = "selected" if t == current else ""
        opts += f'<option value="{t}" {sel}>{t}</option>'
    return f'<label>ticker: <select onchange="location=\'/admin?section={section}&ticker=\'+this.value">{opts}</select></label>'


SECTIONS = [
    "securities", "orderbook", "price_history",
    "ohlcv", "derived", "shareholders", "api_keys", "meta",
]


@admin_router.get("", response_class=HTMLResponse, include_in_schema=False)
@admin_router.get("/", response_class=HTMLResponse, include_in_schema=False)
async def admin_panel(
    section: str = Query(default="securities"),
    ticker: str = Query(default=""),
):
    tickers = await db.get_all_tickers()
    ner_ok = await db.get_meta("ner_reachable") != "false"
    initialized = await db.get_meta("atlas_initialized") == "true"
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    rows = await _get_data(section, ticker or None)

    nav = "".join(
        f'<a href="/admin?section={s}" class="nav{" active" if s == section else ""}">{s}</a>'
        for s in SECTIONS
    )

    filter_html = ""
    if section in ("orderbook", "price_history", "ohlcv", "shareholders"):
        filter_html = f'<span class="filter">{_ticker_sel(tickers, ticker, section)}</span>'

    tf = f"&ticker={ticker}" if ticker else ""
    export_html = f"""<span class="exports">export:
      <a href="/admin/export/{section}?fmt=csv{tf}">csv</a>
      <a href="/admin/export/{section}?fmt=json{tf}">json</a>
      <a href="/admin/export/{section}?fmt=xlsx{tf}">xlsx</a>
    </span>
    <span class="rowcount">{len(rows)} rows</span>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ATLAS / {section}</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: "Courier New", Courier, monospace;
    font-size: 13px;
    background: #fff;
    color: #000;
  }}

  #topbar {{
    border-bottom: 2px solid #000;
    padding: 7px 16px;
    display: flex;
    align-items: baseline;
    gap: 16px;
    flex-wrap: wrap;
  }}
  #topbar .wordmark {{ font-weight: bold; letter-spacing: 4px; font-size: 14px; }}
  #topbar .slash {{ color: #bbb; }}
  #topbar .right {{ margin-left: auto; font-size: 11px; color: #888; display: flex; gap: 16px; align-items: baseline; }}
  #topbar .right a {{ color: #888; text-decoration: none; }}
  #topbar .right a:hover {{ color: #000; }}
  .ner-down {{ text-decoration: line-through; color: #aaa; }}

  #nav {{
    border-bottom: 1px solid #000;
    padding: 5px 16px;
    display: flex;
    gap: 4px;
    flex-wrap: wrap;
  }}
  a.nav {{
    padding: 2px 10px;
    color: #999;
    text-decoration: none;
    font-size: 12px;
    border: 1px solid transparent;
  }}
  a.nav:hover {{ color: #000; }}
  a.nav.active {{
    color: #000;
    border: 1px solid #000;
  }}

  #toolbar {{
    border-bottom: 1px solid #ddd;
    padding: 5px 16px;
    display: flex;
    align-items: center;
    gap: 20px;
    background: #fafafa;
    font-size: 12px;
    flex-wrap: wrap;
  }}
  .filter label {{ color: #555; }}
  .filter select {{
    font-family: inherit;
    font-size: 12px;
    border: 1px solid #000;
    padding: 1px 4px;
    background: #fff;
    margin-left: 4px;
    cursor: pointer;
  }}
  .exports {{ color: #888; }}
  .exports a {{
    color: #000;
    font-weight: bold;
    text-decoration: underline;
    margin-left: 6px;
  }}
  .exports a:hover {{
    background: #000;
    color: #fff;
    text-decoration: none;
    padding: 1px 2px;
  }}
  .rowcount {{ color: #bbb; margin-left: auto; }}

  #section-label {{
    padding: 8px 16px 0;
    font-size: 10px;
    color: #bbb;
    letter-spacing: 3px;
    text-transform: uppercase;
  }}

  .tbl-wrap {{ overflow-x: auto; padding: 0 16px 32px; }}
  table {{ border-collapse: collapse; margin-top: 8px; width: 100%; }}
  thead tr {{ border-bottom: 2px solid #000; }}
  th {{
    text-align: left;
    padding: 4px 16px 4px 0;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    white-space: nowrap;
  }}
  td {{
    padding: 3px 16px 3px 0;
    border-bottom: 1px solid #f0f0f0;
    white-space: nowrap;
    max-width: 260px;
    overflow: hidden;
    text-overflow: ellipsis;
    color: #222;
  }}
  tr:hover td {{ background: #f7f7f7; }}
  .empty {{ padding: 24px 16px; color: #bbb; font-size: 12px; }}
  .truncnote {{ padding: 6px 16px; font-size: 11px; color: #bbb; }}
</style>
</head>
<body>

<div id="topbar">
  <span class="wordmark">ATLAS</span>
  <span class="slash">/</span>
  <span>admin</span>
  <span class="slash">/</span>
  <span>{section}</span>
  <div class="right">
    <span class="{'ner-ok' if ner_ok else 'ner-down'}">NER</span>
    <span>{'ready' if initialized else 'init...'}</span>
    <span>{now}</span>
    <a href="/dashboard">dashboard</a>
    <a href="/docs">api docs</a>
  </div>
</div>

<div id="nav">{nav}</div>

<div id="toolbar">
  {filter_html}
  {export_html}
</div>

<div id="section-label">{section}</div>
<div id="content">{_table(rows)}</div>

</body>
</html>"""

    return HTMLResponse(content=html)
