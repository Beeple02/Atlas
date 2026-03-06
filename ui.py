"""
Shared UI primitives for Atlas admin + dashboard.
Light, minimal, clean typography.
"""

SHARED_CSS = """
@import url('https://fonts.googleapis.com/css2?family=Geist:wght@400;500;600&family=Geist+Mono:wght@400;500&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

body {
  font-family: 'Geist', -apple-system, BlinkMacSystemFont, sans-serif;
  font-size: 14px;
  background: #fff;
  color: #111;
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
}

/* ── Top bar ── */
#topbar {
  height: 48px;
  border-bottom: 1px solid #e5e5e5;
  padding: 0 24px;
  display: flex;
  align-items: center;
  gap: 8px;
  position: sticky;
  top: 0;
  background: #fff;
  z-index: 100;
}
.tb-wordmark {
  font-weight: 600;
  font-size: 14px;
  letter-spacing: 0.01em;
  color: #111;
}
.tb-sep { color: #d4d4d4; font-size: 16px; font-weight: 300; }
.tb-section { color: #737373; font-size: 14px; }
.tb-right {
  margin-left: auto;
  display: flex;
  align-items: center;
  gap: 20px;
  font-size: 12px;
  color: #a3a3a3;
}
.tb-right a {
  color: #737373;
  text-decoration: none;
  font-size: 12px;
}
.tb-right a:hover { color: #111; }
.tb-pill {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  padding: 3px 8px;
  border-radius: 999px;
  font-size: 11px;
  font-weight: 500;
  border: 1px solid;
}
.tb-pill.ok  { color: #16a34a; background: #f0fdf4; border-color: #bbf7d0; }
.tb-pill.err { color: #dc2626; background: #fef2f2; border-color: #fecaca; }
.tb-pill.warn { color: #d97706; background: #fffbeb; border-color: #fde68a; }
.dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; }

/* ── Nav tabs ── */
#nav {
  border-bottom: 1px solid #e5e5e5;
  padding: 0 24px;
  display: flex;
  gap: 0;
  overflow-x: auto;
}
a.nav-tab {
  display: inline-block;
  padding: 10px 14px;
  color: #737373;
  text-decoration: none;
  font-size: 13px;
  border-bottom: 2px solid transparent;
  white-space: nowrap;
  transition: color 0.1s;
  margin-bottom: -1px;
}
a.nav-tab:hover { color: #111; }
a.nav-tab.active {
  color: #111;
  border-bottom-color: #111;
  font-weight: 500;
}

/* ── Toolbar ── */
#toolbar {
  padding: 10px 24px;
  border-bottom: 1px solid #e5e5e5;
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
  background: #fafafa;
}
.tb-label { font-size: 12px; color: #737373; }
select {
  font-family: inherit;
  font-size: 13px;
  border: 1px solid #d4d4d4;
  border-radius: 6px;
  padding: 4px 8px;
  background: #fff;
  color: #111;
  cursor: pointer;
  outline: none;
}
select:hover { border-color: #a3a3a3; }
select:focus { border-color: #111; }

/* Export buttons */
.export-group { display: flex; align-items: center; gap: 6px; }
.export-group .lbl { font-size: 12px; color: #a3a3a3; }
.btn-export {
  display: inline-block;
  padding: 4px 10px;
  font-size: 12px;
  font-weight: 500;
  color: #374151;
  background: #fff;
  border: 1px solid #d4d4d4;
  border-radius: 5px;
  text-decoration: none;
  transition: all 0.1s;
}
.btn-export:hover {
  background: #111;
  color: #fff;
  border-color: #111;
}
.row-count {
  margin-left: auto;
  font-size: 12px;
  color: #a3a3a3;
  font-variant-numeric: tabular-nums;
}

/* ── Content area ── */
#content { padding: 24px; }

/* ── Table ── */
.tbl-wrap { overflow-x: auto; }
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}
thead th {
  text-align: left;
  padding: 8px 12px 8px 0;
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  color: #737373;
  border-bottom: 1px solid #e5e5e5;
  white-space: nowrap;
}
tbody td {
  padding: 9px 12px 9px 0;
  border-bottom: 1px solid #f5f5f5;
  color: #374151;
  font-family: 'Geist Mono', monospace;
  font-size: 12px;
  white-space: nowrap;
  max-width: 240px;
  overflow: hidden;
  text-overflow: ellipsis;
}
tbody td:first-child { font-weight: 500; color: #111; font-family: 'Geist', sans-serif; }
tbody tr:last-child td { border-bottom: none; }
tbody tr:hover td { background: #fafafa; }

.empty-state {
  padding: 48px 0;
  text-align: center;
  color: #a3a3a3;
  font-size: 13px;
}
.trunc-note {
  margin-top: 12px;
  font-size: 12px;
  color: #a3a3a3;
}

/* ── Stat cards ── */
.stat-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
  gap: 1px;
  background: #e5e5e5;
  border: 1px solid #e5e5e5;
  border-radius: 8px;
  overflow: hidden;
  margin-bottom: 32px;
}
.stat-card {
  background: #fff;
  padding: 18px 20px;
}
.stat-card .s-label { font-size: 11px; color: #a3a3a3; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 8px; }
.stat-card .s-value { font-size: 22px; font-weight: 600; color: #111; font-variant-numeric: tabular-nums; }
.stat-card .s-sub { font-size: 11px; color: #a3a3a3; margin-top: 4px; }

/* ── Section block ── */
.section-block {
  border: 1px solid #e5e5e5;
  border-radius: 8px;
  overflow: hidden;
  margin-bottom: 24px;
}
.section-block-header {
  padding: 14px 20px;
  border-bottom: 1px solid #e5e5e5;
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.section-block-header h2 { font-size: 13px; font-weight: 600; color: #111; }
.section-block-body { padding: 0 20px 4px; }

/* ── Status indicators ── */
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 11px;
  font-weight: 500;
}
.badge.green { background: #f0fdf4; color: #16a34a; }
.badge.red   { background: #fef2f2; color: #dc2626; }
.badge.gray  { background: #f5f5f5; color: #737373; }

/* ── Poll status ── */
.poll-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 1px;
  background: #e5e5e5;
  border-radius: 6px;
  overflow: hidden;
}
.poll-item {
  background: #fff;
  padding: 12px 16px;
}
.poll-item .p-label { font-size: 11px; color: #a3a3a3; margin-bottom: 4px; }
.poll-item .p-value { font-size: 13px; color: #111; font-family: 'Geist Mono', monospace; }
"""


def page(title: str, topbar: str, nav: str, toolbar: str, content: str, refresh: int = 0) -> str:
    refresh_tag = f'<meta http-equiv="refresh" content="{refresh}">' if refresh else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
{refresh_tag}
<title>{title}</title>
<style>{SHARED_CSS}</style>
</head>
<body>
{topbar}
{nav}
{toolbar}
{content}
</body>
</html>"""


def topbar(wordmark: str, section: str, right_items: str) -> str:
    return f"""<div id="topbar">
  <span class="tb-wordmark">{wordmark}</span>
  <span class="tb-sep">/</span>
  <span class="tb-section">{section}</span>
  <div class="tb-right">{right_items}</div>
</div>"""
